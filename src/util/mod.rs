use std::io::{Result as IOResult};
use std::path::Path;
use std::fs;
use std::time::Duration;
use humansize::{FileSize, file_size_opts as fso};

mod accum;

pub use accum::DataAccumulator;

pub fn human_time(dur: Duration) -> String {
  let secs = dur.as_secs_f32();
  if secs > 3600.0 {
    let hrs = secs / 3600.0;
    let mins = (secs % 3600.0) / 60.0;
    let secs = secs % 60.0;
    format!("{}h{}m{:.2}s", hrs.floor(), mins.floor(), secs)
  } else if secs > 60.0 {
    let mins = secs / 60.0;
    let secs = secs % 60.0;
    format!("{}m{:.2}s", mins.floor(), secs)
  } else {
    format!("{:.2}s", secs)
  }
}


pub fn human_size<S: FileSize>(size: S) -> String {
  size.file_size(fso::BINARY).unwrap()
}

pub fn file_human_size<P: AsRef<Path>>(path: P) -> IOResult<String> {
  let meta = fs::metadata(path)?;
  Ok(human_size(meta.len()))
}
