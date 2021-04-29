use anyhow::Result;

/// Trait for writing objects to some kind of sink.
pub trait ObjectWriter<T> {
  /// Write one object.
  fn write_object(&mut self, object: &T) -> Result<()>;

  /// Finish and close the target.
  fn finish(self) -> Result<usize>;
}
