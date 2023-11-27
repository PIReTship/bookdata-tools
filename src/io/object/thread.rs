use std::thread::{spawn, JoinHandle};

use anyhow::{anyhow, Result};
use crossbeam::channel::{bounded, Sender};

use super::ObjectWriter;

/// Write objects in a background thread.
pub struct ThreadObjectWriter<T>
where
    T: Send + Sync + 'static,
{
    sender: Sender<T>,
    handle: JoinHandle<Result<usize>>,
}

impl<T: Send + Sync + 'static> ThreadObjectWriter<T> {
    pub fn new<W: ObjectWriter<T> + Send + 'static>(writer: W) -> ThreadObjectWriter<T> {
        Self::with_capacity(writer, 4096)
    }

    pub fn with_capacity<W: ObjectWriter<T> + Send + 'static>(
        writer: W,
        cap: usize,
    ) -> ThreadObjectWriter<T> {
        let (sender, receiver) = bounded(cap);

        let handle = spawn(|| {
            let recv = receiver;
            let mut writer = writer; // move writer into thread

            for obj in recv.iter() {
                writer.write_object(obj)?;
            }

            writer.finish()
        });

        ThreadObjectWriter { sender, handle }
    }
}

impl<T: Send + Sync + 'static> ObjectWriter<T> for ThreadObjectWriter<T> {
    fn write_object(&mut self, object: T) -> Result<()> {
        self.sender.send(object)?;
        Ok(())
    }

    fn finish(self) -> Result<usize> {
        drop(self.sender);
        let res = self.handle.join();
        res.unwrap_or_else(|e| Err(anyhow!("background thread panicked: {:?}", e)))
    }
}
