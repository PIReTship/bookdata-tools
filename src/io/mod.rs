use std::io::prelude::*;
use std::io::BufReader;
use std::fs::File;
use std::path::Path;

use anyhow::Result;
use indicatif::ProgressBar;
use flate2::bufread::MultiGzDecoder;

pub mod progress;
pub mod hash;
pub mod delim;
pub mod lines;

pub use hash::{HashRead, HashWrite};
pub use delim::DelimPrinter;
pub use lines::LineProcessor;

/// Open a gzip-compressed file for input, with a progress bar.
pub fn open_gzin_progress<P: AsRef<Path>>(path: P) -> Result<(Box<dyn BufRead>, ProgressBar)> {
  let read = File::open(path)?;
  let pb = progress::default_progress(read.metadata()?.len());
  let pbr = pb.wrap_read(read);
  let pbr = BufReader::new(pbr);
  let gzf = MultiGzDecoder::new(pbr);
  let bfs = BufReader::new(gzf);
  Ok((Box::new(bfs), pb))
}
