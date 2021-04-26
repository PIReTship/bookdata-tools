use std::io::prelude::*;
use std::io::{BufReader, Lines};
use std::fs::File;
use std::error::Error;
use std::str::FromStr;
use std::path::Path;
use std::marker::PhantomData;

use anyhow::Result;
use indicatif::ProgressBar;
use happylog::{LogPBState, set_progress};
use flate2::bufread::MultiGzDecoder;

use super::progress::default_progress_style;

/// Read lines from a file with buffering, decompression, and parsing.
pub struct LineProcessor {
  #[allow(dead_code)]
  progress: ProgressBar,
  reader: Box<dyn BufRead>,
  log_state: Option<LogPBState>,
}

pub struct Records<R> {
  lines: Lines<Box<dyn BufRead>>,
  #[allow(dead_code)]
  log_state: Option<LogPBState>,
  phantom: PhantomData<R>
}

impl <R: FromStr> Iterator for Records<R> where R::Err : 'static + Error + Send + Sync {
  type Item = Result<R>;

  fn next(&mut self) -> Option<Self::Item> {
    self.lines.next().map(|l| {
      let line = l?;
      let rec = line.parse()?;
      Ok(rec)
    })
  }
}

impl LineProcessor {
  /// Open a line processor from a gzipped source.
  pub fn open_gzip<P: AsRef<Path>>(path: P) -> Result<LineProcessor> {
    let read = File::open(path)?;
    let progress = ProgressBar::new(read.metadata()?.len());
    progress.set_style(default_progress_style());
    let read = progress.wrap_read(read);
    let read = BufReader::new(read);
    let read = MultiGzDecoder::new(read);
    let read = BufReader::new(read);
    let state = set_progress(&progress);
    Ok(LineProcessor {
      progress,
      reader: Box::new(read),
      log_state: Some(state)
    })
  }

  /// Get the lines as strings.
  #[allow(dead_code)]
  pub fn lines(self) -> Lines<Box<dyn BufRead>> {
    self.reader.lines()
  }

  /// Get the lines as records.
  pub fn records<R: FromStr>(self) -> Records<R> {
    Records {
      lines: self.reader.lines(),
      phantom: PhantomData,
      log_state: self.log_state
    }
  }
}
