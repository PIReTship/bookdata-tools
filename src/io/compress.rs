use std::io::prelude::*;
use std::io::{self, BufReader, copy};
use std::fs::File;
use std::path::Path;
use std::thread::{spawn, JoinHandle};

use log::*;
use anyhow::{Result, anyhow};
use indicatif::ProgressBar;
use flate2::bufread::MultiGzDecoder;
use zip::read::*;
use os_pipe::{pipe, PipeReader};

use super::progress::default_progress;

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
pub fn open_gzin_progress<P: AsRef<Path>>(path: P) -> Result<(Box<dyn BufRead>, ProgressBar)> {
  let read = File::open(path)?;
  let pb = default_progress(read.metadata()?.len());
  let pbr = pb.wrap_read(read);
  let pbr = BufReader::new(pbr);
  let gzf = MultiGzDecoder::new(pbr);

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
pub fn open_solo_zip<P: AsRef<Path>>(path: P) -> Result<(Box<dyn BufRead>, ProgressBar)> {
  let pstr = path.as_ref().to_string_lossy();
  info!("opening zip file from {}", pstr);
  let file = File::open(path.as_ref())?;
  let file = BufReader::new(file);
  let zf = ZipArchive::new(file)?;
  if zf.len() > 1 {
    error!("{}: more than one member file", pstr);
    return Err(anyhow!("too many input files"))
  } else if zf.len() == 0 {
    error!("{}: empty input archive", pstr);
    return Err(anyhow!("empty input archive"));
  }
  let pb = default_progress(1024);
  let pb2 = pb.clone(); // make a copy for the thread to own
  let (read, write) = pipe()?;

  let handle = spawn(|| {
    // move in the things we will own
    let pb = pb2;
    let mut zf = zf;
    let mut dst = write;

    // open and read the member
    debug!("opening member from file");
    let member = zf.by_index(0)?;
    info!("processing member {:?} with {} bytes", member.name(), member.size());
    pb.set_length(member.size());
    let mut read = pb.wrap_read(member);
    copy(&mut read, &mut dst)
  });

  let thr = ThreadRead {
    read, handle: Some(handle)
  };
  let bfs = BufReader::new(thr);
  Ok((Box::new(bfs), pb))
}
