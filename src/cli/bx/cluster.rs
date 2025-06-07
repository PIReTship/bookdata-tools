//! BookCrossing interaction clustering.
use std::path::PathBuf;

use crate::prelude::*;
use polars::prelude::*;

#[derive(Args, Debug)]
#[command(name = "cluster-actions")]
pub struct Cluster {
    /// Cluster ratings.
    #[arg(long = "ratings")]
    ratings: bool,

    /// Cluster actions (implicit feedback).
    #[arg(long = "add-actions")]
    add_actions: bool,

    /// The output file.
    #[arg(short = 'o', long = "output", name = "FILE")]
    outfile: PathBuf,
}

impl Command for Cluster {
    fn exec(&self) -> Result<()> {
        if !self.ratings && !self.add_actions {
            error!("one of --ratings or --add-actions must be specified");
            return Err(anyhow!("no mode specified"));
        }
        require_working_dir("bx")?;

        let isbns = LazyFrame::scan_parquet("../book-links/isbn-clusters.parquet", default())?;
        let isbns = isbns.select(&[col("isbn"), col("cluster")]);

        let ratings = LazyCsvReader::new("cleaned-ratings.csv")
            .has_header(true)
            .finish()?;
        let ratings = if self.ratings {
            ratings.filter(col("rating").gt(0))
        } else {
            ratings
        };
        let joined = ratings.join(
            isbns,
            &[col("isbn")],
            &[col("isbn")],
            JoinType::Inner.into(),
        );
        let grouped = joined.group_by(&[
            col("user").alias("user_id"),
            col("cluster").alias("item_id"),
        ]);
        let agg = if self.ratings {
            grouped.agg(&[
                col("rating").median().alias("rating"),
                col("cluster").count().alias("nratings"),
            ])
        } else {
            grouped.agg(&[col("cluster").count().alias("nactions")])
        };

        info!("collecting results");
        let results = agg.collect()?;

        info!("writing to {:?}", &self.outfile);
        save_df_parquet(results, &self.outfile)?;

        Ok(())
    }
}
