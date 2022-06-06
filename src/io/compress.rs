use std::io::prelude::*;
use std::io::{self, BufReader, copy};
use std::fs::File;
use std::path::Path;
use std::thread::{spawn, JoinHandle};

use log::*;
use anyhow::{Result, anyhow};
use flate2::bufread::MultiGzDecoder;
use zip::read::*;
use indicatif::{ProgressBar, ProgressStyle};
use os_pipe::{pipe, PipeReader};

use crate::util::Timer;

static FILE_PROGRESS: &'static str = "{prefix}: {bar} {pos}/{len} ({elapsed} elapsed, ETA {eta})";

struct ThreadRead {
  read: PipeReader,
  handle: Option<JoinHandle<io::Result<u64>>>
}

impl Read for ThreadRead {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let size = self.read.read(buf)?;
    if size == 0 {
      // eof - make sure thread completed successfully
      // this makes it so thread failure results in an error
      // on the last call to `read`, instead of a panic when
      // the reader is dropped.
      if let Some(h) = self.handle.take() {
        let res = h.join().expect("thread error");
        let sz = res?;
        debug!("thread copied {} bytes", sz);
      }
    }
    Ok(size)
  }
}

/// Open a gzip-compressed file for input, with a progress bar.
pub fn open_gzin_progress(path: &Path) -> Result<(impl BufRead, ProgressBar)> {
  let name = path.file_name().unwrap().to_string_lossy();
  let read = File::open(path)?;
  let pb = ProgressBar::new(read.metadata()?.len());
  let pb = pb.with_style(ProgressStyle::default_bar().template(FILE_PROGRESS));

  let read = Timer::builder().label(&name).interval(30.0).log_target(module_path!()).file_progress(read)?;
  let read = pb.wrap_read(read);
  let read = BufReader::new(read);
  let gzf = MultiGzDecoder::new(read);

  let (read, writer) = pipe()?;

  let jh = spawn(|| {
    let mut src = gzf;
    let mut dst = writer;
    copy(&mut src, &mut dst)
  });

  let thr = ThreadRead {
    read, handle: Some(jh)
  };
  let bfs = BufReader::new(thr);
  Ok((Box::new(bfs), pb))
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

  let handle = spawn(|| {
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

  let thr = ThreadRead {
    read, handle: Some(handle)
  };
  let bfs = BufReader::new(thr);
  Ok(Box::new(bfs))
}
