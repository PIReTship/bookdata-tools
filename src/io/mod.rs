use std::path::Path;
use std::io::{Result as IOResult};
use std::fs;

pub mod progress;
pub mod delim;
pub mod compress;
pub mod lines;
pub mod object;

pub use delim::DelimPrinter;
pub use lines::LineProcessor;
pub use compress::open_gzin_progress;
pub use object::ObjectWriter;

pub fn file_size<P: AsRef<Path>>(path: P) -> IOResult<u64> {
  let meta = fs::metadata(path)?;
  Ok(meta.len())
}
