use std::path::{PathBuf, Path};
use std::fs::{OpenOptions, read_to_string};
use std::collections::HashMap;

use tokio;

use bookdata::prelude::*;

use serde::Deserialize;
use toml::from_str;
use arrow::record_batch::RecordBatch;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::arrow::ArrowWriter;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::DFSchema;

use happylog::set_progress;

/// Run a DataFusion SQL query and save its results.
#[derive(StructOpt, Debug)]
#[structopt(name="fusion")]
pub struct Fusion {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Output file name.
  #[structopt(short="-o", long="output")]
  output: Option<String>,

  /// Operation specification to run.
  #[structopt(name = "SPEC", parse(from_os_str))]
  spec: PathBuf
}

#[derive(Deserialize)]
struct JobSpec {
  script: String,
  tables: HashMap<String,String>
}

#[tokio::main]
pub async fn main() -> Result<()> {
  let opts = Fusion::from_args();
  opts.common.init()?;

  let mut ctx = ExecutionContext::new();

  info!("reading spec from {}", &opts.spec.to_string_lossy());
  let spec = read_to_string(&opts.spec)?;
  let spec: JobSpec = from_str(&spec)?;

  info!("reading script from {}", &spec.script);
  let script = read_to_string(&spec.script)?;

  for (name, path) in spec.tables {
    info!("attaching table {} from {}", name, path);
    ctx.register_parquet(&name, &path)?;
  }

  if let Some(out_fn) = &opts.output {
    info!("planning script");
    let lplan = ctx.create_logical_plan(&script)?;
    let plan = ctx.create_physical_plan(&lplan)?;

    info!("executing script to output file");
    let props = WriterProperties::builder();
    let props = props.set_compression(Compression::ZSTD);
    let props = props.build();
    ctx.write_parquet(plan, out_fn.to_owned(), Some(props)).await?;
  } else {
    info!("preparing SQL script");
    let df = ctx.sql(&script)?;
    info!("executing script");
    let results = df.collect().await?;
    arrow::util::pretty::pretty_format_batches(&results)?;
  }

  Ok(())
}
