use std::io::prelude::*;
use std::io::{BufReader, Lines};
use std::fs::File;
use std::path::Path;

use anyhow::Result;
use indicatif::ProgressBar;
use happylog::{LogPBState, set_progress};
use flate2::bufread::MultiGzDecoder;

use super::progress::default_progress_style;

pub struct LineProcessor {
  progress: ProgressBar,
  reader: Box<dyn BufRead>
}

impl LineProcessor {
  /// Open a line processor from a gzipped source
  fn open_gzip<P: AsRef<Path>>(path: P) -> Result<LineProcessor> {
    let read = File::open(path)?;
    let progress = ProgressBar::new(read.metadata()?.len());
    progress.set_style(default_progress_style());
    let read = progress.wrap_read(read);
    let read = BufReader::new(read);
    let read = MultiGzDecoder::new(read);
    let read = BufReader::new(read);
    Ok(LineProcessor {
      progress,
      reader: Box::new(read)
    })
  }

  /// Register progress bar with logger
  fn wire_logger(&self) -> LogPBState {
    set_progress(&self.progress)
  }
}
