use std::io::prelude::*;
use std::io::{Lines};
use std::error::Error;
use std::str::FromStr;
use std::path::Path;
use std::marker::PhantomData;

use log::*;
use anyhow::Result;
use indicatif::ProgressBar;
use happylog::{LogPBState, set_progress};
use serde::de::DeserializeOwned;

use super::compress::open_gzin_progress;
use super::ObjectWriter;

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

pub struct JSONRecords<R> {
  lines: Lines<Box<dyn BufRead>>,
  #[allow(dead_code)]
  log_state: Option<LogPBState>,
  phantom: PhantomData<R>
}

impl <R: DeserializeOwned> Iterator for JSONRecords<R> {
  type Item = Result<R>;

  fn next(&mut self) -> Option<Self::Item> {
    self.lines.next().map(|l| {
      let line = l?;
      let rec = serde_json::from_str(&line)?;
      Ok(rec)
    })
  }
}

impl LineProcessor {
  /// Open a line processor from a gzipped source.
  pub fn open_gzip<P: AsRef<Path>>(path: P) -> Result<LineProcessor> {
    let (read, progress) = open_gzin_progress(path)?;
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

  /// Get the lines as JSON records.
  pub fn json_records<R: DeserializeOwned>(self) -> JSONRecords<R> {
    JSONRecords {
      lines: self.reader.lines(),
      phantom: PhantomData,
      log_state: self.log_state
    }
  }

  /// Process JSON rows into an object writer.
  ///
  /// This parses each line of the data set, deserializes it with JSON, and passes the resulting object
  /// to the specified [ObjectWriter].  It produces useful error messages with line numbers when there
  /// is a failure. It does **not** call [ObjectWriter::finish] when it is done - the caller needs to do that.
  pub fn process_json<W, R>(self, writer: &mut W) -> Result<usize> where R: DeserializeOwned, W: ObjectWriter<R> {
    let mut line_no = 0;
    let pb = self.progress.clone();
    for line in self.json_records() {
      line_no += 1;
      let obj: R = line.map_err(|e| {
        error!("error parsing line {}: {:?}", line_no, e);
        e
      })?;
      writer.write_object(obj).map_err(|e| {
        error!("error writing line {}: {:?}", line_no, e);
        e
      })?;
    }

    if pb.length() != pb.position() {
      warn!("reading file did not consume all input");
    }

    debug!("read {} lines in {} bytes", line_no, pb.position());
    pb.finish_and_clear();

    Ok(line_no)
  }
}
