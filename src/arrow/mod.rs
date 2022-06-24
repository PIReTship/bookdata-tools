pub mod reader;
pub mod writer;
pub mod batch_io;
pub mod dfext;

pub use reader::scan_parquet_file;
pub use parquet::record::RecordWriter;
pub use parquet::file::reader::FileReader;
pub use arrow2_convert::ArrowField;
pub use arrow2_convert::serialize::ArrowSerialize;
pub use writer::TableWriter;

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
