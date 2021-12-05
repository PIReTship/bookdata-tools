//! Collect ISBNs from across the data sources.
use std::fs::read_to_string;
use std::fmt::Debug;
use std::collections::HashMap;
use futures::StreamExt;
use datafusion::prelude::*;

use serde::Deserialize;
use toml;
use async_trait::async_trait;

use arrow::array::*;
use friendly::bytes;

use crate::prelude::*;
use crate::ids::collector::KeyCollector;

use super::AsyncCommand;

/// Collect ISBNs from across the data sources.
#[derive(StructOpt, Debug)]
#[structopt(name="collect-isbns")]
pub struct CollectISBNs {
  /// Path to the output file (in Parquet format)
  #[structopt(short="o", long="output")]
  out_file: PathBuf,

  /// path to the ISBN source definition file (in TOML format)
  #[structopt(name="DEFS")]
  source_file: PathBuf
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum MultiSource {
  Single(ISBNSource),
  Multi(Vec<ISBNSource>),
}

#[derive(Deserialize, Debug)]
struct ISBNSource {
  path: String,
  #[serde(default)]
  column: Option<String>,
}

/// The type of ISBN source set specifications.
type SourceSet = HashMap<String, MultiSource>;

impl MultiSource {
  fn to_list(&self) -> Vec<&ISBNSource> {
    match self {
      MultiSource::Single(s) => vec![&s],
      MultiSource::Multi(ss) => ss.iter().map(|s| s).collect(),
    }
  }
}

/// Read a single ISBN source into the accumulator.
async fn read_source(ctx: &mut ExecutionContext, kc: &mut KeyCollector, name: &str, src: &ISBNSource) -> Result<()> {
  let mut acc = kc.accum(name);
  let id_col = src.column.as_deref().unwrap_or("isbn");

  info!("reading ISBNs from {} (column {})", src.path, id_col);

  let df = if src.path.ends_with(".csv") {
    let opts = CsvReadOptions::new().has_header(true);
    ctx.read_csv(&src.path, opts).await?
  } else {
    ctx.read_parquet(&src.path).await?
  };
  let df = df.select(vec![
    col(id_col).alias("isbn")
  ])?;
  let df = df.filter(col("isbn").is_not_null())?;

  let mut batches = df.execute_stream().await?;
  while let Some(batch) = batches.next().await {
    let batch = batch?;
    let col = batch.column(0);
    let col = col.as_any().downcast_ref::<StringArray>().expect("failed to downcast");
    acc.add_keys(col.iter().flatten());
  }

  async fn exec_future(&self) -> Result<()> {
    info!("reading spec from {}", self.source_file.display());
    let spec = read_to_string(&self.source_file)?;
    let spec: SourceSet = toml::de::from_str(&spec)?;

    let mut ctx = ExecutionContext::new();
    let mut kc = KeyCollector::new();
    for (name, ms) in spec {
      for source in ms.to_list() {
        read_source(&mut ctx, &mut kc, &name, source).await?;
      }
    }

    info!("saving {} ISBNs to {}", kc.len(), self.out_file.display());
    let n = kc.save("isbn", "isbn_id", &self.out_file)?;
    info!("wrote {} rows in {}", n, bytes(file_size(&self.out_file)?));

    Ok(())
  }
}
