use std::path::{Path, PathBuf};
use std::io::{Result as IOResult};
use std::fs;

pub mod progress;
pub mod compress;
pub mod lines;
pub mod object;

pub use lines::LineProcessor;
pub use compress::open_gzin_progress;
pub use object::ObjectWriter;

/// Trait for data processing sinks with input and ouptut files.
pub trait DataSink {
  /// Get the output files for the sink.
  fn output_files(&self) -> Vec<PathBuf>;

  /// Get auxillary input files for the sink.
  ///
  /// Most sinks are also an [ObjectWriter], and the primary input is written
  /// to the sink; that input file is not reported here.  However, sinks may
  /// require additional input files to process, and those files can be reported
  /// here.
  fn input_files(&self) -> Vec<PathBuf> {
    Vec::new()
  }
}

/// Convert a list of strings into owned [PathBuf]s.
pub fn path_list(paths: &[&str]) -> Vec<PathBuf> {
  paths.into_iter().map(|p| PathBuf::from(p)).collect()
}

/// Get the size of a file.
pub fn file_size<P: AsRef<Path>>(path: P) -> IOResult<u64> {
  let meta = fs::metadata(path)?;
  Ok(meta.len())
}
