use std::thread::{spawn, JoinHandle, Scope, ScopedJoinHandle};

use anyhow::Result;
use crossbeam::channel::{bounded, Receiver, Sender};

use super::ObjectWriter;

enum WorkHandle<'scope> {
    Static(JoinHandle<Result<usize>>),
    Scoped(ScopedJoinHandle<'scope, Result<usize>>),
}

fn ferry<T, W>(recv: Receiver<T>, writer: W) -> Result<usize>
where
    T: Send + Sync + 'static,
    W: ObjectWriter<T>,
{
    let mut writer = writer; // move writer into thread

    for obj in recv.iter() {
        writer.write_object(obj)?;
    }

    writer.finish()
}

/// Write objects in a background thread.
pub struct ThreadObjectWriter<'scope, T>
where
    T: Send + Sync + 'static,
{
    sender: Sender<T>,
    handle: WorkHandle<'scope>,
}

impl<T: Send + Sync + 'static> ThreadObjectWriter<'static, T> {
    pub fn new<W: ObjectWriter<T> + Send + 'static>(writer: W) -> ThreadObjectWriter<'static, T> {
        Self::with_capacity(writer, 4096)
    }

    pub fn with_capacity<W: ObjectWriter<T> + Send + 'static>(
        mut writer: W,
        cap: usize,
    ) -> ThreadObjectWriter<'static, T> {
        let (sender, receiver) = bounded(cap);

        let handle = spawn(|| ferry(receiver, writer));

        ThreadObjectWriter {
            sender,
            handle: WorkHandle::Static(handle),
        }
    }
}

impl<'scope, T: Send + Sync + 'static> ThreadObjectWriter<'scope, T> {
    pub fn with_scope<'env, W>(
        writer: &'env mut W,
        scope: &'scope Scope<'scope, 'env>,
        cap: usize,
    ) -> ThreadObjectWriter<'scope, T>
    where
        W: ObjectWriter<T> + Send + Sync,
        'env: 'scope,
    {
        let (sender, receiver) = bounded(cap);

        let handle = scope.spawn(move || ferry(receiver, writer));

        ThreadObjectWriter {
            sender,
            handle: WorkHandle::Scoped(handle),
        }
    }

    pub fn child<'a>(&'a self) -> ThreadChildWriter<'a, 'scope, T>
    where
        'scope: 'a,
    {
        ThreadChildWriter::create(self)
    }
}

impl<'scope, T: Send + Sync + 'static> ObjectWriter<T> for ThreadObjectWriter<'scope, T> {
    fn write_object(&mut self, object: T) -> Result<()> {
        self.sender.send(object)?;
        Ok(())
    }

    fn finish(self) -> Result<usize> {
        drop(self.sender);
        let res = match self.handle {
            WorkHandle::Static(h) => h.join().map_err(std::panic::resume_unwind)?,
            WorkHandle::Scoped(h) => h.join().map_err(std::panic::resume_unwind)?,
        };
        res
    }
}

/// Child writer to allow objects to be written to a threaded writer from multiple
/// threads, allowing parallel feeding of data.  It takes a **reference** to the
/// original thread writer, so it needs to be used with [std::thread::scope]. The
/// original writer still needs to be closed, and this design ensures that the
/// original cannot be closed until all the children are finished.
#[derive(Clone)]
pub struct ThreadChildWriter<'a, 'scope, T>
where
    T: Send + Sync + 'static,
    'scope: 'a,
{
    delegate: &'a ThreadObjectWriter<'scope, T>,
    sender: Sender<T>,
}

impl<'a, 'scope, T> ThreadChildWriter<'a, 'scope, T>
where
    T: Send + Sync + 'static,
    'scope: 'a,
{
    /// Create a new thread child writer.
    fn create(delegate: &'a ThreadObjectWriter<'scope, T>) -> ThreadChildWriter<'a, 'scope, T> {
        ThreadChildWriter {
            delegate,
            sender: delegate.sender.clone(),
        }
    }
}

impl<'a, 'scope, T> ObjectWriter<T> for ThreadChildWriter<'a, 'scope, T>
where
    T: Send + Sync + 'static,
    'scope: 'a,
{
    fn write_object(&mut self, object: T) -> Result<()> {
        self.sender.send(object)?;
        Ok(())
    }

    fn finish(self) -> Result<usize> {
        // do nothing, dropping the writer will close the sender
        Ok(0)
    }
}
