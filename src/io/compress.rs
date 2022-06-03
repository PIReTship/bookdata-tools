use std::io::prelude::*;
use std::io::{BufReader, copy};
use std::fs::File;
use std::path::Path;
use std::thread::spawn;

use log::*;
use anyhow::{Result, anyhow};
use flate2::bufread::MultiGzDecoder;
use zip::read::*;
use os_pipe::pipe;

use crate::util::Timer;
use super::background::ThreadRead;

/// Open a gzip-compressed file for input, with a progress bar.
pub fn open_gzin_progress(path: &Path) -> Result<impl BufRead> {
  let name = path.file_name().unwrap().to_string_lossy();
  let read = File::open(path)?;
  let read = Timer::builder().label(&name).interval(30.0).log_target(module_path!()).file_progress(read)?;
  let read = BufReader::new(read);
  let gzf = MultiGzDecoder::new(read);

  let thr = ThreadRead::new(gzf)?;
  let bfs = BufReader::new(thr);
  Ok(Box::new(bfs))
}

/// Open a zingle member from a zip file for input, with a progress bar.
///
/// Some of our data source (particularly Library of Congress linked data files) are
/// delived as ZIP archives containing a single member.  This function opens such a file
/// as a reader.  Zip decompression is handled in a background thread.
pub fn open_solo_zip(path: &Path) -> Result<impl BufRead> {
  let pstr = path.to_string_lossy();
  info!("opening zip file from {}", pstr);
  let file = File::open(path)?;
  let file = BufReader::new(file);
  let zf = ZipArchive::new(file)?;
  if zf.len() > 1 {
    error!("{}: more than one member file", pstr);
    return Err(anyhow!("too many input files"))
  } else if zf.len() == 0 {
    error!("{}: empty input archive", pstr);
    return Err(anyhow!("empty input archive"));
  }
  let (read, write) = pipe()?;

  let handle = spawn(move || {
    // move in the things we will own
    let mut zf = zf;
    let mut dst = write;

    // open and read the member
    debug!("opening member from file");
    let member = zf.by_index(0)?;
    info!("processing member {:?} with {} bytes", member.name(), member.size());
    let mut timer = Timer::builder();
    timer.log_target(module_path!());
    timer.task_count(member.size() as usize);
    timer.label(member.name());
    timer.interval(30.0);
    let mut read = timer.read_progress(member);
    copy(&mut read, &mut dst)
  });

  let thr = ThreadRead::create(read, handle);
  let bfs = BufReader::new(thr);
  Ok(Box::new(bfs))
}
