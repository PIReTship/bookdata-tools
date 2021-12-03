//! Scan Amazon ratings or reviews.
use futures::stream::{StreamExt};

use datafusion::prelude::*;
use crate::prelude::*;
use crate::arrow::fusion::*;
use crate::interactions::*;
use crate::cli::AsyncCommand;
use async_trait::async_trait;

use anyhow::Result;

mod data;
mod amazon;
mod bx;
mod goodreads;

use data::Source;

/// Cluster ratings or interactions
#[derive(StructOpt, Debug)]
#[structopt(name="group-actions")]
pub struct ClusterActions {
  /// The data to cluster.
  #[structopt(short="s", long="source")]
  source: String,

  /// Use native works instead of clusters (if supported).
  #[structopt(long="native-works")]
  native_works: bool,

  /// Input file (to override per-source default)
  // This is a string in because that's what DataFusion expects.
  #[structopt(short="f", long="input-file")]
  infile: Option<String>,

  /// Rating output file
  #[structopt(short="o", long="output", name="FILE", parse(from_os_str))]
  outfile: PathBuf,
}

async fn scan_and_save<S: Source>(src: S, ctx: &mut ExecutionContext, dst: &Path) -> Result<usize> {
  let source = src.scan_linked_actions(ctx).await?;

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

  dedup.save(dst)
}

#[async_trait]
impl AsyncCommand for ClusterActions {
  async fn exec_future(&self) -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let infile = self.infile.as_ref().map(|s| s.as_str());

    let dst = self.outfile.as_ref();
    match self.source.as_str() {
      "az-ratings" => scan_and_save(amazon::Ratings::new(infile), &mut ctx, dst).await?,
      "bx-ratings" => scan_and_save(bx::Ratings, &mut ctx, dst).await?,
      "bx-actions" => scan_and_save(bx::Actions, &mut ctx, dst).await?,
      "gr-ratings" => scan_and_save(goodreads::Ratings::new(self.native_works, infile), &mut ctx, dst).await?,
      "gr-actions" => scan_and_save(goodreads::Actions::new(self.native_works, infile), &mut ctx, dst).await?,
      s => return Err(anyhow!("invalid data source {}", s))
    };

    Ok(())
  }
}
