use std::io::prelude::*;
use std::thread::{spawn, JoinHandle};
use std::mem::drop;

use serde::Serialize;
use csv;
use std::sync::mpsc::{sync_channel, SyncSender};
use anyhow::{anyhow, Result};

/// Trait for writing objects to some kind of sink.
pub trait ObjectWriter<T> {
  /// Write one object.
  fn write_object(&mut self, object: T) -> Result<()>;

  /// Finish and close the target.
  fn finish(self) -> Result<usize>;
}

/// Write objects in a background thread.
pub struct ThreadWriter<T> where T: Send + Sync + 'static {
  sender: SyncSender<T>,
  handle: JoinHandle<Result<usize>>
}

impl <T: Send + Sync + 'static> ThreadWriter<T> {
  pub fn new<W: ObjectWriter<T> + Send + 'static>(writer: W) -> ThreadWriter<T> {
    let (sender, receiver) = sync_channel(4096);

    let handle = spawn(|| {
      let recv = receiver;
      let mut writer = writer;  // move writer into thread

      for obj in recv.iter() {
        writer.write_object(obj)?;
      }

      writer.finish()
    });

    ThreadWriter {
      sender, handle
    }
  }
}

impl <T: Send + Sync + 'static> ObjectWriter<T> for ThreadWriter<T> {
  fn write_object(&mut self, object: T) -> Result<()> {
    self.sender.send(object)?;
    Ok(())
  }

  fn finish(self) -> Result<usize> {
    drop(self.sender);
    let res = self.handle.join();
    res.unwrap_or_else(|e| {
      Err(anyhow!("background thread panicked: {:?}", e))
    })
  }
}


impl <T: Serialize, W: Write> ObjectWriter<T> for csv::Writer<W> {
  fn write_object(&mut self, object: T) -> Result<()> {
    self.serialize(object)?;
    Ok(())
  }

  fn finish(mut self) -> Result<usize> {
    self.flush()?;
    drop(self);
    Ok(0)
  }
}
