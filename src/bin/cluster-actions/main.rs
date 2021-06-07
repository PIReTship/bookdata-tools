//! Scan Amazon ratings or reviews.
use std::sync::Arc;

use parse_display::{Display, FromStr};
use futures::stream::{StreamExt};

use serde::{Deserialize};

use tokio;

use datafusion::prelude::*;
use bookdata::prelude::*;
use bookdata::arrow::fusion::*;
use bookdata::ratings::RatingDedup;

use anyhow::Result;

mod amazon;

#[derive(Display, FromStr, Debug)]
#[display(style="kebab-case")]
enum SourceName {
  AzRatings,
}

trait Source {
  fn scan_linked_actions(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>>;

  fn has_timestamps(&self) -> bool;
  fn is_ratings(&self) -> bool;
}

impl SourceName {
  fn resolve(&self) -> impl Source {
    match self {
      SourceName::AzRatings => amazon::Ratings
    }
  }
}

/// Cluster ratings or interactions
#[derive(StructOpt)]
#[structopt(name="cluster-actions")]
pub struct ClusterActions {
  #[structopt(flatten)]
  common: CommonOpts,

  /// The data to coluster.
  #[structopt(short="s", long="source")]
  source: SourceName,

  /// Rating output file
  #[structopt(short="o", long="output", name="FILE", parse(from_os_str))]
  outfile: PathBuf,
}

#[derive(Deserialize)]
struct RatingRow {
  user: u32,
  cluster: i32,
  rating: f32,
  timestamp: i64
}

async fn scan_ratings_dedup(ctx: &mut ExecutionContext, source: Arc<dyn DataFrame>) -> Result<RatingDedup> {
  let mut dedup = RatingDedup::new();

  let mut timer = Timer::new();
  let mut rows = df_rows(ctx, source).await?;
  info!("scanning ratings for deduplication");
  while let Some(row) = rows.next().await {
    let row: RatingRow = row?;
    dedup.record(row.user, row.cluster, row.rating, row.timestamp);
    timer.complete(1);
    timer.log_status("scanning ratings", 5.0);
  }

  Ok(dedup)
}

#[tokio::main]
async fn main() -> Result<()> {
  let opts = ClusterActions::from_args();
  opts.common.init()?;

  let mut ctx = ExecutionContext::new();

  let source = opts.source.resolve();
  let rlink = source.scan_linked_actions(&mut ctx)?;

  if source.is_ratings() {
    let dedup = scan_ratings_dedup(&mut ctx, rlink).await?;

    // now we manually scan the results to build our final results.
    // this is because of two deficiencies in DataFusion:
    // - no median function
    // - inefficient high-cardinality group-by (they're working on this)
    let n = dedup.write_ratings(opts.outfile, true)?;
    info!("wrote {} de-duplicated ratings", n);
  } else {
    panic!("not implemented");
  }

  Ok(())
}
