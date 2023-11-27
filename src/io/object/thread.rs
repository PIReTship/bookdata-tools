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
        writer: W,
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

    /// Create a satellite writer that writes to the same backend as this
    /// writer.
    ///
    /// Satellites can be used to enable multiple data-generating threads to
    /// write to the same thread writer, turning it into a multi-producer,
    /// single-consumer writing pipeline.  Satellite writers should be finished,
    /// and closing them does not finish the original thread writer (it still
    /// needs to have [ObjectWriter::finish] called, typically after all
    /// satellites are done, but it calling [ObjectWriter::finish] while
    /// satellites are still active will wait until the satellites have finished
    /// and closed their connections to the consumer thread).
    ///
    /// Satellites hold a reference to the original thread writer, to discourage
    /// keeping them alive after the thread writer has been finished.  They
    /// work best with [std::thread::scope]:
    ///
    /// ```rust,ignore
    /// let writer = ThreadWriter::new(writer);
    /// scope(|s| {
    ///     for i in 0..NTHREADS {
    ///         let out = writer.satellite();
    ///         s.spawn(move || {
    ///             // process and write to out
    ///             out.finish().expect("closing writer failed");
    ///         })
    ///     }
    /// })
    /// ```
    pub fn satellite<'a>(&'a self) -> ThreadWriterSatellite<'a, 'scope, T>
    where
        'scope: 'a,
    {
        ThreadWriterSatellite::create(self)
    }
}

impl<'scope, T: Send + Sync + 'static> ObjectWriter<T> for ThreadObjectWriter<'scope, T> {
    fn write_object(&mut self, object: T) -> Result<()> {
        self.sender.send(object)?;
        Ok(())
    }

    fn finish(self) -> Result<usize> {
        // dropping the sender will cause the consumer to hang up.
        drop(self.sender);
        // wait for the consumer to finish before we consider the writer closed.
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
pub struct ThreadWriterSatellite<'a, 'scope, T>
where
    T: Send + Sync + 'static,
    'scope: 'a,
{
    _delegate: &'a ThreadObjectWriter<'scope, T>,
    sender: Sender<T>,
}

impl<'a, 'scope, T> ThreadWriterSatellite<'a, 'scope, T>
where
    T: Send + Sync + 'static,
    'scope: 'a,
{
    /// Create a new thread child writer.
    fn create(delegate: &'a ThreadObjectWriter<'scope, T>) -> ThreadWriterSatellite<'a, 'scope, T> {
        ThreadWriterSatellite {
            _delegate: delegate,
            sender: delegate.sender.clone(),
        }
    }
}

impl<'a, 'scope, T> ObjectWriter<T> for ThreadWriterSatellite<'a, 'scope, T>
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
