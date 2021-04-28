use std::io::prelude::*;
use std::io::{self, BufReader, copy};
use std::fs::File;
use std::path::Path;
use std::thread::{spawn, JoinHandle};

use log::*;
use anyhow::Result;
use indicatif::ProgressBar;
use flate2::bufread::MultiGzDecoder;
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
