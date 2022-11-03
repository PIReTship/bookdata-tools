use std::io::prelude::*;
use std::io::{BufReader, copy};
use std::fs::File;
use std::path::Path;
use std::thread::spawn;

use log::*;
use anyhow::{Result, anyhow};
use flate2::bufread::MultiGzDecoder;
use zip::read::*;
use indicatif::ProgressBar;
use os_pipe::pipe;
use friendly::bytes;

use crate::util::Timer;
use crate::io::background::ThreadRead;
use super::open_progress;

/// Open a gzip-compressed file for input, with a progress bar.
///
/// It sets the progress bar's prefix to the file name.
pub fn open_gzin_progress(path: &Path, pb: ProgressBar) -> Result<impl BufRead> {
  let read = open_progress(path, pb)?;
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
    let mut member = zf.by_index(0)?;
    info!("processing member {:?} with {} bytes", member.name(), member.size());
    let timer = Timer::new();
    let res = copy(&mut member, &mut dst);
    if let Ok(size) = res {
      info!("copied {} in {}", bytes(size), timer);
    }
    res
  });

  let thr = ThreadRead::create(read, handle);
  let bfs = BufReader::new(thr);
  Ok(Box::new(bfs))
}
