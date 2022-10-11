//! Collect ISBNs from across the data sources.
use std::fs::read_to_string;
use std::fmt::Debug;
use std::collections::HashMap;

use serde::Deserialize;
use toml;

use friendly::bytes;

use polars::prelude::*;
use crate::prelude::*;
use crate::prelude::Result;
use crate::ids::collector::KeyCollector;

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
fn read_source(kc: &mut KeyCollector, name: &str, src: &ISBNSource) -> Result<()> {
  let mut acc = kc.accum(name);
  let id_col = src.column.as_deref().unwrap_or("isbn");

  info!("reading ISBNs from {} (column {})", src.path, id_col);

  let df = if src.path.ends_with(".csv") {
    LazyCsvReader::new(src.path.to_string()).has_header(true).finish()?
  } else {
    LazyFrame::scan_parquet(src.path.to_string(), Default::default())?
  };
  let df = df.select(&[col(id_col)]);
  let df = df.drop_nulls(None);

  let df = df.collect()?;

  let isbns = df.column(id_col)?.utf8()?;

  info!("adding {} ISBNs", isbns.len());
  acc.add_keys(isbns.into_iter().flatten());

  Ok(())
}

impl Command for CollectISBNs {
  fn exec(&self) -> Result<()> {
    info!("reading spec from {}", self.source_file.display());
    let spec = read_to_string(&self.source_file)?;
    let spec: SourceSet = toml::de::from_str(&spec)?;

    let mut kc = KeyCollector::new();
    for (name, ms) in spec {
      for source in ms.to_list() {
        read_source(&mut kc, &name, source)?;
      }
    }

    info!("saving {} ISBNs to {}", kc.len(), self.out_file.display());
    let n = kc.save("isbn", "isbn_id", &self.out_file)?;
    info!("wrote {} rows in {}", n, bytes(file_size(&self.out_file)?));

    Ok(())
  }
}
