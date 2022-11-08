//! BookCrossing interaction clustering.
use std::path::{PathBuf};

use structopt::StructOpt;

use polars::prelude::*;
use crate::prelude::*;

#[derive(StructOpt, Debug)]
#[structopt(name="cluster-actions")]
pub struct Cluster {
  /// Cluster ratings.
  #[structopt(long="ratings")]
  ratings: bool,

  /// Cluster actions (implicit feedback).
  #[structopt(long="add-actions")]
  add_actions: bool,

  /// The output file.
  #[structopt(short="o", long="output", name="FILE")]
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

    let ratings = LazyCsvReader::new("cleaned-ratings.csv").has_header(true).finish()?;
    let ratings = if self.ratings {
      ratings.filter(col("rating").gt(0))
    } else {
      ratings
    };
    let joined = ratings.join(isbns, &[col("isbn")], &[col("isbn")], JoinType::Inner);
    let grouped = joined.groupby(&[
      col("user"), col("cluster").alias("item")
    ]);
    let agg = if self.ratings {
      grouped.agg(&[
        col("rating").median().alias("rating"),
        col("cluster").count().alias("nratings"),
      ])
    } else {
      grouped.agg(&[
        col("cluster").count().alias("nactions"),
      ])
    };

    info!("collecting results");
    let results = agg.collect()?;

    info!("writing to {:?}", &self.outfile);
    save_df_parquet(results, &self.outfile)?;

    Ok(())
  }
}
