pub mod types;
pub mod table;
pub mod row_de;
pub mod writer;
pub mod fusion;

pub use row_de::RecordBatchDeserializer;
pub use row_de::scan_parquet_file;
pub use table::TableRow;
pub use writer::{TableWriter, TableWriterBuilder};
pub use types::*;
pub use parquet::file::reader::FileReader;

use std::path::Path;
use std::fs::File;

use anyhow::Result;
use parquet::file::reader::SerializedFileReader;

/// Convenience function for opening a Parquet reader.
pub fn open_parquet_file<P: AsRef<Path>>(path: P) -> Result<SerializedFileReader<File>> {
  let file = File::open(path)?;
  let read = SerializedFileReader::new(file)?;
  Ok(read)
}
