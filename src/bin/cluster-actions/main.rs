//! Scan Amazon ratings or reviews.
use futures::stream::{StreamExt};

use tokio;

use datafusion::prelude::*;
use bookdata::prelude::*;
use bookdata::arrow::fusion::*;
use bookdata::ratings::*;

use anyhow::Result;

mod data;
mod amazon;
mod bx;

use data::Source;

/// Cluster ratings or interactions
#[derive(StructOpt)]
#[structopt(name="cluster-actions")]
pub struct ClusterActions {
  #[structopt(flatten)]
  common: CommonOpts,

  /// The data to cluster.
  #[structopt(short="s", long="source")]
  source: String,

  /// Rating output file
  #[structopt(short="o", long="output", name="FILE", parse(from_os_str))]
  outfile: PathBuf,
}

async fn scan_and_save<S: Source>(src: S, ctx: &mut ExecutionContext, dst: &Path) -> Result<usize> {
  let source = src.scan_linked_actions(ctx)?;

  let mut dedup = src.make_dedup();

  let mut timer = Timer::new();
  let mut rows = df_rows(ctx, source).await?;
  info!("scanning ratings for deduplication");
  while let Some(row) = rows.next().await {
    let row: S::Act = row?;
    dedup.add_interaction(row)?;
    timer.complete(1);
    timer.log_status("scanning ratings", 5.0);
  }

  dedup.save(dst, src.has_timestamps())
}

#[tokio::main]
async fn main() -> Result<()> {
  let opts = ClusterActions::from_args();
  opts.common.init()?;

  let mut ctx = ExecutionContext::new();

  let dst = opts.outfile.as_ref();
  match opts.source.as_str() {
    "az-ratings" => scan_and_save(amazon::Ratings, &mut ctx, dst).await?,
    "bx-ratings" => scan_and_save(bx::Ratings, &mut ctx, dst).await?,
    "bx-actions" => scan_and_save(bx::Actions, &mut ctx, dst).await?,
    s => return Err(anyhow!("invalid data source {}", s))
  };

  Ok(())
}
