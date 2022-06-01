//! Extract basic information from a Parquet file.
use std::io::{Write, stdout};
use std::mem::drop;
use std::fs::File;
use std::fmt::Debug;

use serde::Serialize;

use arrow::datatypes::Schema;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::reader::{SerializedFileReader, FileReader};
use friendly::{bytes, scalar};

use crate::prelude::*;

use super::Command;

/// Extract basic information from a Parquet file.
#[derive(StructOpt, Debug)]
#[structopt(name="collect-isbns")]
pub struct PQInfo {
  /// Path to the output JSON file.
  #[structopt(short="o", long="output")]
  out_file: Option<PathBuf>,

  /// Path to the Parquet file.
  #[structopt(name="FILE")]
  source_file: PathBuf
}

#[derive(Debug, Serialize)]
struct InfoStruct {
  row_count: i64,
  file_size: u64,
  #[serde(flatten)]
  schema: Schema,
}

impl Command for PQInfo {
  fn exec(&self) -> Result<()> {
    info!("reading {:?}", self.source_file);

    let pqf = File::open(&self.source_file)?;
    let fmeta = pqf.metadata()?;
    info!("file size: {}", bytes(fmeta.len()));

    let pqr = SerializedFileReader::new(pqf)?;
    let meta = pqr.metadata();
    let file_meta = meta.file_metadata();
    info!("row count: {}", scalar(file_meta.num_rows()));

    info!("decoding schema");
    let schema = file_meta.schema_descr();
    let arrow_schema = parquet_to_arrow_schema(schema, None)?;

    let out = stdout();
    let mut ol = out.lock();
    write!(&mut ol, "{}\n", arrow_schema)?;
    drop(ol);

    if let Some(ref file) = self.out_file {
      let mut out = File::create(file)?;
      let info = InfoStruct {
        row_count: file_meta.num_rows(),
        file_size: fmeta.len(),
        schema: arrow_schema,
      };

      serde_json::to_writer_pretty(&mut out, &info)?;
    }

    Ok(())
  }
}
