pub mod types;
pub mod row_de;
pub mod writer;
pub mod writer2;
pub mod batch_io;
pub mod fusion;

pub use row_de::RecordBatchDeserializer;
pub use row_de::scan_parquet_file;
pub use parquet::record::RecordWriter;
pub use parquet::file::reader::FileReader;
pub use parquet_derive::ParquetRecordWriter;
pub use arrow2_convert::ArrowField;
pub use writer::{TableWriter, TableWriterBuilder};
pub use types::*;

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
