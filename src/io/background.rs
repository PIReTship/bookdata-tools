use std::io::prelude::*;
use std::io::{self, copy};
use std::mem::drop;
use std::thread::{spawn, JoinHandle};

use anyhow::{Result};
use log::*;
use os_pipe::{pipe, PipeReader, PipeWriter};
use friendly::bytes;

use crate::util::timing::Timer;

/// Reader that reads from a background thread.
pub struct ThreadRead {
  read: PipeReader,
  handle: Option<JoinHandle<io::Result<u64>>>
}

/// Writer that writes to a background thread.
pub struct ThreadWrite {
  write: Option<PipeWriter>,
  handle: Option<JoinHandle<io::Result<u64>>>
}

impl ThreadRead {
  /// Push a read into a background thread.
  pub fn new<R: Read + Send + 'static>(chan: R) -> Result<ThreadRead> {
    let (read, writer) = pipe()?;

    let jh = spawn(move || {
      let mut src = chan;
      let mut dst = writer;
      let timer = Timer::new();
      let res = copy(&mut src, &mut dst);
      if let Ok(size) = res {
        info!("copied {} in {}", bytes(size), timer);
      }
      res
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

impl ThreadWrite {
  /// Push a read into a background thread.
  pub fn new<W: Write + Send + 'static>(chan: W) -> Result<ThreadWrite> {
    let (read, writer) = pipe()?;

    let jh = spawn(move || {
      let mut src = read;
      let mut dst = chan;
      copy(&mut src, &mut dst)
    });

    Ok(ThreadWrite {
      write: Some(writer), handle: Some(jh)
    })
  }

  fn do_close(&mut self) -> io::Result<u64> {
    if let Some(mut write) = self.write.take() {
      write.flush()?;
      drop(write);
    }
    if let Some(h) = self.handle.take() {
      let res = h.join().expect("thread error");
      let sz = res?;
      debug!("thread copied {} bytes", sz);
      Ok(sz)
    } else {
      Err(io::ErrorKind::BrokenPipe.into())
    }
  }

  /// Close and drop the thread writer.
  #[allow(dead_code)]
  pub fn close(mut self) -> io::Result<u64> {
    self.do_close()
  }
}

impl Drop for ThreadWrite {
  fn drop(&mut self) {
    if self.write.is_some() || self.handle.is_some() {
      self.do_close().expect("unclosed background thread failed");
    }
  }
}

impl Write for ThreadWrite {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    if let Some(ref mut write) = self.write {
      write.write(buf)
    } else {
      Err(io::ErrorKind::BrokenPipe.into())
    }
  }

  /// Flush the thread writer. This is not reliable.
  fn flush(&mut self) -> io::Result<()> {
    if let Some(ref mut write) = self.write {
      write.flush()
    } else {
      Err(io::ErrorKind::BrokenPipe.into())
    }
  }
}
