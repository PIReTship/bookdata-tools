use std::path::{PathBuf};
use std::time::Instant;
use std::fs::{File, read_to_string};
use std::future::Future;
use std::sync::Arc;

use tokio;
use tokio::runtime::Runtime;

use bookdata::prelude::*;
use bookdata::arrow::fusion::*;
use bookdata::graph::load_graph;

use flate2::write::GzEncoder;
use molt::*;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::{ExecutionPlan};

/// Run the book clustering algorithm.
#[derive(StructOpt, Debug)]
#[structopt(name="cluster-books")]
pub struct ClusterBooks {
  #[structopt(flatten)]
  common: CommonOpts,
}

#[tokio::main]
pub async fn main() -> Result<()> {
  let opts = ClusterBooks::from_args();
  opts.common.init()?;

  let graph = load_graph().await?;

  Ok(())
}
