//! Cluster Amazon ratings.
use polars::prelude::*;
use crate::prelude::*;


/// Group Amazon ratings into clusters.
#[derive(StructOpt, Debug)]
#[structopt(name="cluster-ratings")]
pub struct ClusterRatings {
  /// Rating output file
  #[structopt(short="o", long="output", name="FILE", parse(from_os_str))]
  ratings_out: PathBuf,

  /// Input file to cluster
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf,
}

impl Command for ClusterRatings {
  fn exec(&self) -> Result<()> {
    let isbns = LazyFrame::scan_parquet("book-links/isbn-clusters.parquet", default())?;
    let isbns = isbns.select(&[col("isbn"), col("cluster")]);

    let ratings = LazyFrame::scan_parquet(&self.infile, default())?;

    let joined = ratings.join(isbns, &[col("asin")], &[col("isbn")], JoinType::Inner);
    let joined = joined.select(&[
      col("user"),
      col("cluster").alias("item"),
      col("rating"),
      col("timestamp"),
    ]).sort("timestamp", default());

    let actions = joined.groupby(&[
      col("user"), col("item")
    ]).agg(&[
      col("rating").median().alias("rating"),
      col("rating").last().alias("last_rating"),
      col("timestamp").min().alias("first_time"),
      col("timestamp").max().alias("last_time"),
      col("item").count().alias("nratings"),
    ]);

    info!("collecting results");
    let actions = actions.collect()?;

    info!("saving {} records", actions.height());
    save_df_parquet(actions, &self.ratings_out)?;

    Ok(())
  }
}
