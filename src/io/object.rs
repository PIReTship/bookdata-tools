use std::io::prelude::*;
use std::marker::PhantomData;
use std::thread::{spawn, JoinHandle};
use std::mem::drop;

use serde::Serialize;
use csv;
use crossbeam::channel::{bounded, Sender};
use anyhow::{anyhow, Result};

/// Trait for writing objects to some kind of sink.
pub trait ObjectWriter<T>: Sized {
  /// Write one object.
  fn write_object(&mut self, object: T) -> Result<()>;

  /// Write an iterator full of objects.
  fn write_all_objects<I>(&mut self, objects: I) -> Result<usize> where I: Iterator<Item=T> {
    let mut count = 0;
    for obj in objects {
      self.write_object(obj)?;
      count += 1;
    }
    Ok(count)
  }

  /// Write an iterator of objects and finish the writer.
  fn write_and_finish<I>(mut self, objects: I) -> Result<usize> where I: Iterator<Item=T> {
    let n = self.write_all_objects(objects)?;
    self.finish()?;
    Ok(n)
  }

  /// Finish and close the target.
  fn finish(self) -> Result<usize>;

  /// Wrap this object writer in a transformed writer.
  fn with_transform<F, T2>(self, transform: F) -> MapWriter<F, T2, T, Self> where F: Fn(T2) -> Result<T> {
    MapWriter { _phantom: PhantomData, transform, writer: self }
  }
}

/// Writer that applies a transform to an underlying writer.
pub struct MapWriter<F, T1, T2, W>
where W: ObjectWriter<T2>,
  F: Fn(T1) -> Result<T2>
{
  _phantom: PhantomData<(T1, T2)>,
  transform: F,
  writer: W,
}

impl <F, T1, T2, W> ObjectWriter<T1> for MapWriter<F, T1, T2, W>
where W: ObjectWriter<T2>,
  F: Fn(T1) -> Result<T2>
{
  fn write_object(&mut self, object: T1) -> Result<()> {
    let mapped = (self.transform)(object)?;
    self.writer.write_object(mapped)
  }

  fn finish(self) -> Result<usize> {
    self.writer.finish()
  }
}

/// Write objects in a background thread.
pub struct ThreadObjectWriter<T> where T: Send + Sync + 'static {
  sender: Sender<T>,
  handle: JoinHandle<Result<usize>>
}

impl <T: Send + Sync + 'static> ThreadObjectWriter<T> {
  pub fn new<W: ObjectWriter<T> + Send + 'static>(writer: W) -> ThreadObjectWriter<T> {
    let (sender, receiver) = bounded(4096);

    let handle = spawn(|| {
      let recv = receiver;
      let mut writer = writer;  // move writer into thread

      for obj in recv.iter() {
        writer.write_object(obj)?;
      }

      writer.finish()
    });

    ThreadObjectWriter {
      sender, handle
    }
  }
}

impl <T: Send + Sync + 'static> ObjectWriter<T> for ThreadObjectWriter<T> {
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
