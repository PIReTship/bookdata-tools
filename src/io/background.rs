use std::io::prelude::*;
use std::io::{self, copy};
use std::thread::{spawn, JoinHandle};

use anyhow::Result;
use log::*;
use os_pipe::{pipe, PipeReader};

pub struct ThreadRead {
  read: PipeReader,
  handle: Option<JoinHandle<io::Result<u64>>>
}

impl ThreadRead {
  /// Push a read into a background thread.
  pub fn new<R: Read + Send + 'static>(chan: R) -> Result<ThreadRead> {
    let (read, writer) = pipe()?;

    let jh = spawn(move || {
      let mut src = chan;
      let mut dst = writer;
      copy(&mut src, &mut dst)
    });

    Ok(ThreadRead {
      read, handle: Some(jh)
    })
  }

  /// Create a thread read from an already-existing thread.
  pub fn create(read: PipeReader, handle: JoinHandle<io::Result<u64>>) -> ThreadRead {
    ThreadRead {
      read, handle: Some(handle)
    }
  }
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
