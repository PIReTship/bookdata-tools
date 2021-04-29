use std::path::Path;
use std::fs::{File, OpenOptions};
use std::sync::Arc;

use log::*;
use arrow::array::ArrayRef;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::arrow::ArrowWriter;
use anyhow::Result;

use crate::io::object::ObjectWriter;

pub use bd_macros::TableRow;

const BATCH_SIZE: usize = 1000000;

/// Trait for serializing records into Parquet tables.
///
/// This trait can be derived:
///
/// ```ignore
/// #[derive(TableRow)]
/// struct Record {
///     user_id: u32,
///     book_id: u64,
///     rating: f32
/// }
/// ```
pub trait TableRow {
  type Batch;

  /// Get the schema
  fn schema() -> Schema;

  /// Create a new Batch Builder
  fn new_batch(cap: usize) -> Self::Batch;

  /// Get a batch's arrays and reset the builder.
  fn finish_batch(batch: &mut Self::Batch) -> Vec<ArrayRef>;

  /// Add a record to a batch builder
  fn write_to_batch(&self, batch: &mut Self::Batch) -> Result<()>;
}

/// Parquet table writer
pub struct TableWriter<R: TableRow> {
  schema: SchemaRef,
  writer: ArrowWriter<File>,
  batch: R::Batch,
  batch_count: usize,
  row_count: usize
}

impl <R> TableWriter<R> where R: TableRow {
  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
    let schema = R::schema();
    let schema = Arc::new(schema);
    let file = OpenOptions::new().create(true).truncate(true).write(true).open(path)?;
    let props = WriterProperties::builder();
    let props = props.set_compression(Compression::ZSTD);
    let props = props.build();
    let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    Ok(TableWriter {
      schema, writer,
      batch: R::new_batch(BATCH_SIZE),
      batch_count: 0,
      row_count: 0,
    })
  }

  fn write_batch(&mut self) -> Result<()> {
    debug!("writing batch");
    let cols = R::finish_batch(&mut self.batch);
    let batch = RecordBatch::try_new(self.schema.clone(), cols)?;
    self.writer.write(&batch)?;
    self.batch_count = 0;
    Ok(())
  }
}

impl <R> ObjectWriter<R> for TableWriter<R> where R: TableRow {
  fn write_object(&mut self, row: R) -> Result<()> {
    row.write_to_batch(&mut self.batch)?;
    self.batch_count += 1;
    self.row_count += 1;
    if self.batch_count >= BATCH_SIZE {
      self.write_batch()?;
    }
    Ok(())
  }

  fn finish(mut self) -> Result<usize> {
    if self.batch_count > 0 {
      self.write_batch()?;
    }
    info!("closing Parquet writer");
    self.writer.close()?;
    Ok(self.row_count)
  }
}
