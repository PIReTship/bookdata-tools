use std::io::prelude::*;
use std::io::BufReader;
use std::fs::File;
use std::path::Path;

use super::progress::default_progress;
use anyhow::Result;
use indicatif::ProgressBar;
use flate2::bufread::MultiGzDecoder;

/// Open a gzip-compressed file for input, with a progress bar.
pub fn open_gzin_progress<P: AsRef<Path>>(path: P) -> Result<(Box<dyn BufRead>, ProgressBar)> {
    let read = File::open(path)?;
    let pb = default_progress(read.metadata()?.len());
    let pbr = pb.wrap_read(read);
    let pbr = BufReader::new(pbr);
    let gzf = MultiGzDecoder::new(pbr);
    let bfs = BufReader::new(gzf);
    Ok((Box::new(bfs), pb))
  }
