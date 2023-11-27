use std::io::prelude::*;
use std::marker::PhantomData;
use std::mem::drop;

use anyhow::Result;
use csv;
use serde::Serialize;

mod chunks;
mod thread;
mod transform;

pub use chunks::{ChunkWriter, UnchunkWriter};
pub use thread::ThreadObjectWriter;
pub use transform::MapWriter;

/// Trait for writing objects to some kind of sink.
pub trait ObjectWriter<T>: Sized {
    /// Write one object.
    fn write_object(&mut self, object: T) -> Result<()>;

    /// Write an iterator full of objects.
    fn write_all_objects<I>(&mut self, objects: I) -> Result<usize>
    where
        I: Iterator<Item = T>,
    {
        let mut count = 0;
        for obj in objects {
            self.write_object(obj)?;
            count += 1;
        }
        Ok(count)
    }

    /// Write an iterator of objects and finish the writer.
    fn write_and_finish<I>(mut self, objects: I) -> Result<usize>
    where
        I: Iterator<Item = T>,
    {
        let n = self.write_all_objects(objects)?;
        self.finish()?;
        Ok(n)
    }

    /// Finish and close the target.
    fn finish(self) -> Result<usize>;

    /// Wrap this object writer in a transformed writer.
    fn with_transform<F, T2>(self, transform: F) -> MapWriter<F, T2, T, Self>
    where
        F: Fn(T2) -> Result<T>,
    {
        MapWriter {
            _phantom: PhantomData,
            transform,
            writer: self,
        }
    }
}

impl<T: Serialize, W: Write> ObjectWriter<T> for csv::Writer<W> {
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
