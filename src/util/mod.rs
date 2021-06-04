use std::io::{Result as IOResult};
use std::path::Path;
use std::fs;
use humansize::{FileSize, file_size_opts as fso};

mod accum;
pub mod timing;
pub mod serde_string;

pub use accum::DataAccumulator;
pub use timing::{human_time, Timer};

pub fn human_size<S: FileSize>(size: S) -> String {
  size.file_size(fso::BINARY).unwrap()
}

pub fn file_human_size<P: AsRef<Path>>(path: P) -> IOResult<String> {
  let meta = fs::metadata(path)?;
  Ok(human_size(meta.len()))
}
