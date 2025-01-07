//! Cluster Amazon ratings.
use crate::prelude::*;
use polars::prelude::*;

/// Group Amazon ratings into clusters.
#[derive(Args, Debug)]
#[command(name = "cluster-ratings")]
pub struct ClusterRatings {
    /// Rating output file
    #[arg(short = 'o', long = "output", name = "FILE")]
    ratings_out: PathBuf,

    /// Input file to cluster
    #[arg(name = "INPUT")]
    infile: PathBuf,
}

impl Command for ClusterRatings {
    fn exec(&self) -> Result<()> {
        let isbns = LazyFrame::scan_parquet("book-links/isbn-clusters.parquet", default())?;
        let isbns = isbns.select(&[col("isbn"), col("cluster")]);

        let ratings = LazyFrame::scan_parquet(&self.infile, default())?;

        let joined = ratings.join(
            isbns,
            &[col("asin")],
            &[col("isbn")],
            JoinType::Inner.into(),
        );
        let joined = joined
            .select(&[
                col("user_id"),
                col("cluster").alias("item_id"),
                col("rating"),
                col("timestamp"),
            ])
            .sort("timestamp", default());

        let actions = joined.group_by(&[col("user_id"), col("item_id")]).agg(&[
            col("rating").median().alias("rating"),
            col("rating").last().alias("last_rating"),
            col("timestamp").min().alias("first_time"),
            col("timestamp").max().alias("last_time"),
            col("item_id").count().alias("nratings"),
        ]);

        info!("collecting results");
        let actions = actions.collect()?;

        info!("saving {} records", actions.height());
        save_df_parquet(actions, &self.ratings_out)?;

        Ok(())
    }
}
