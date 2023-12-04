use std::marker::PhantomData;

use anyhow::Result;

use super::ObjectWriter;

/// Writer that applies a transform to an underlying writer.
pub struct MapWriter<F, T1, T2, W>
where
    W: ObjectWriter<T2>,
    F: Fn(T1) -> Result<T2>,
{
    pub(crate) _phantom: PhantomData<(T1, T2)>,
    pub(crate) transform: F,
    pub(crate) writer: W,
}

impl<F, T1, T2, W> ObjectWriter<T1> for MapWriter<F, T1, T2, W>
where
    W: ObjectWriter<T2>,
    F: Fn(T1) -> Result<T2>,
{
    fn write_object(&mut self, object: T1) -> Result<()> {
        let mapped = (self.transform)(object)?;
        self.writer.write_object(mapped)
    }

    fn finish(self) -> Result<usize> {
        self.writer.finish()
    }
}
