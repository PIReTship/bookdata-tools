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

const BATCH_SIZE: usize = 1000000;

/// Trait for serializing records into batches
pub trait TableWrite {
  type Batch;
  type Record;

  /// Create a new Batch Builder
  fn new_builder(cap: usize) -> Self::Batch;

  /// Get a batch's arrays and reset the builder.
  fn finish(&self, batch: &mut Self::Batch) -> Vec<ArrayRef>;

  /// Get the schema
  fn schema() -> Schema;

  /// Add a record to a batch builder
  fn write(&mut self, row: &Self::Record, batch: &mut Self::Batch) -> Result<()>;
}

/// Parquet table writer
pub struct TableWriter<'a, W: TableWrite> {
  tablew: &'a mut W,
  schema: SchemaRef,
  writer: ArrowWriter<File>,
  batch: W::Batch,
  batch_count: usize,
  row_count: usize
}

impl <'a, W> TableWriter<'a, W> where W: TableWrite {
  pub fn open<P: AsRef<Path>>(path: P, tw: &'a mut W) -> Result<Self> {
    let schema = W::schema();
    let schema = Arc::new(schema);
    let file = OpenOptions::new().create(true).truncate(true).write(true).open(path)?;
    let props = WriterProperties::builder();
    let props = props.set_compression(Compression::ZSTD);
    let props = props.build();
    let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    Ok(TableWriter {
      tablew: tw, schema, writer,
      batch: W::new_builder(BATCH_SIZE),
      batch_count: 0,
      row_count: 0,
    })
  }

  pub fn write(&mut self, row: &W::Record) -> Result<()> {
    self.tablew.write(row, &mut self.batch)?;
    self.batch_count += 1;
    self.row_count += 1;
    if self.batch_count >= BATCH_SIZE {
      self.write_batch()?;
    }
    Ok(())
  }

  fn write_batch(&mut self) -> Result<()> {
    debug!("writing batch");
    let cols = self.tablew.finish(&mut self.batch);
    let batch = RecordBatch::try_new(self.schema.clone(), cols)?;
    self.writer.write(&batch)?;
    self.batch_count = 0;
    Ok(())
  }

  pub fn finish(mut self) -> Result<usize> {
    if self.batch_count > 0 {
      self.write_batch()?;
    }
    info!("closing Parquet writer");
    self.writer.close()?;
    Ok(self.row_count)
  }
}
