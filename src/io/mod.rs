use std::path::{Path, PathBuf};
use std::io::{Result as IOResult, BufReader, BufRead};
use std::fs;
use friendly::bytes;
use log::*;
use indicatif::ProgressBar;

pub mod background;
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

/// Log the sizes of a set of files.
pub fn log_file_info<P: AsRef<Path>, S: IntoIterator<Item=P>>(files: S) -> IOResult<()> {
  for path in files {
    let path = path.as_ref();
    let size = file_size(path)?;
    info!("output {:?}: {}", path, bytes(size));
  }

  Ok(())
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

/// Open a file as a buffered reader with a progress bar.
pub fn open_progress(path: &Path, pb: ProgressBar) -> IOResult<impl BufRead> {
  let name = path.file_name().unwrap().to_string_lossy();
  let read = fs::File::open(path)?;
  pb.set_length(read.metadata()?.len());
  pb.set_prefix(name.to_string());

  let read = pb.wrap_read(read);
  let read = BufReader::new(read);
  Ok(read)
}
