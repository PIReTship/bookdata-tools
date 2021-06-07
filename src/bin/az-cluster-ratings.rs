//! Scan Amazon ratings or reviews.
use std::sync::Arc;

use futures::stream::{StreamExt};

use serde::{Deserialize};

use tokio;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use datafusion::prelude::*;
use bookdata::prelude::*;
use bookdata::arrow::fusion::*;
use bookdata::ratings::RatingDedup;

use anyhow::Result;

/// Scan an Amazon source file into Parquet.
#[derive(StructOpt)]
#[structopt(name="az-cluster-ratings")]
pub struct ClusterRatings {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Show the logical plan for the data frame and exit.
  #[structopt(long="logical-plan")]
  logical_plan: bool,

  /// Show the physical plan for the data frame and exit.
  #[structopt(long="physical-plan")]
  physical_plan: bool,

  /// Rating output file
  #[structopt(short="o", long="output", name="FILE", parse(from_os_str))]
  outfile: PathBuf,

  /// Rating input file
  #[structopt(name = "INPUT")]
  infile: String,
}

#[derive(Deserialize)]
struct ScanRow {
  user: u32,
  cluster: i32,
  rating: f32,
  timestamp: i64
}

async fn scan_dedup(ctx: &mut ExecutionContext, source: Arc<dyn DataFrame>) -> Result<RatingDedup> {
  let mut dedup = RatingDedup::new();

  let mut timer = Timer::new();
  let mut rows = df_rows(ctx, source).await?;
  info!("scanning ratings for deduplication");
  while let Some(row) = rows.next().await {
    let row: ScanRow = row?;
    dedup.record(row.user, row.cluster, row.rating, row.timestamp);
    timer.complete(1);
    timer.log_status("scanning ratings", 5.0);
  }

  Ok(dedup)
}

#[tokio::main]
async fn main() -> Result<()> {
  let opts = ClusterRatings::from_args();
  opts.common.init()?;

  let mut ctx = ExecutionContext::new();

  // load linking info
  let ic_link = ctx.read_parquet("book-links/isbn-clusters.parquet")?;
  let ic_link = ic_link.select_columns(&["isbn", "cluster"])?;

  // load ratings
  let ratings = ctx.read_parquet(opts.infile.as_str())?;
  let rlink = ratings.join(ic_link, JoinType::Inner, &["asin"], &["isbn"])?;
  let rlink = rlink.select_columns(&["user", "cluster", "rating", "timestamp"])?;

  if opts.logical_plan {
    let plan = rlink.to_logical_plan();
    let plan = ctx.optimize(&plan)?;
    println!("Logical plan:\n{}", plan.display_indent());
    return Ok(())
  } else if opts.physical_plan {
    let exp = rlink.explain(true)?;
    let res: Vec<RecordBatch> = exp.collect().await?;
    print_batches(&res)?;
    return Ok(())
  }

  let dedup = scan_dedup(&mut ctx, rlink).await?;

  // now we manually scan the results to build our final results.
  // this is because of two deficiencies in DataFusion:
  // - no median function
  // - inefficient high-cardinality group-by (they're working on this)
  let n = dedup.write_ratings(opts.outfile)?;
  info!("wrote {} de-duplicated ratings", n);

  Ok(())
}
