use std::path::Path;
use std::fs::OpenOptions;
use std::sync::Arc;
use std::marker::PhantomData;

use log::*;
use arrow::datatypes::{Schema, SchemaRef, Field};
use arrow::record_batch::RecordBatch;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::ParquetWriter;
use parquet::arrow::ArrowWriter;
use anyhow::{Result, anyhow};

use crate::io::object::{ObjectWriter, ThreadWriter};
use super::table::TableRow;

const BATCH_SIZE: usize = 1000000;

/// Parquet table writer
pub struct TableWriter<R: TableRow> {
  schema: SchemaRef,
  writer: Option<ThreadWriter<RecordBatch>>,
  batch: R::Batch,
  batch_size: usize,
  batch_count: usize,
  row_count: usize
}

/// Builder for Parquet table writers.
#[derive(Clone)]
pub struct TableWriterBuilder<R: TableRow> {
  phantom: PhantomData<R>,
  schema: SchemaRef,
  bsize: usize
}

impl <W> ObjectWriter<RecordBatch> for ArrowWriter<W> where W: ParquetWriter + 'static {
  fn write_object(&mut self, batch: RecordBatch) -> Result<()> {
    self.write(&batch)?;
    Ok(())
  }

  fn finish(mut self) -> Result<usize> {
    self.close()?;
    Ok(0)
  }
}

impl <R> TableWriterBuilder<R> where R: TableRow {
  pub fn new() -> TableWriterBuilder<R> {
    TableWriterBuilder {
      phantom: PhantomData,
      schema: Arc::new(R::schema()),
      bsize: BATCH_SIZE
    }
  }

  pub fn rename<'a>(&'a mut self, orig: &str, tgt: &str) -> &'a mut TableWriterBuilder<R> {
    let fields = self.schema.fields();
    let mut f2 = Vec::with_capacity(fields.len());
    for f in fields {
      if f.name() == orig {
        f2.push(Field::new(tgt, f.data_type().clone(), f.is_nullable()))
      } else {
        f2.push(f.clone())
      }
    }
    self.schema = Arc::new(Schema::new(f2));
    self
  }

  pub fn batch_size<'a>(&'a mut self, size: usize) -> &'a mut TableWriterBuilder<R> {
    self.bsize = size;
    self
  }

  pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<TableWriter<R>> {
    let file = OpenOptions::new().create(true).truncate(true).write(true).open(path)?;
    let props = WriterProperties::builder();
    let props = props.set_compression(Compression::ZSTD);
    let props = props.build();
    let writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;
    let writer = ThreadWriter::new(writer);
    Ok(TableWriter {
      schema: self.schema.clone(),
      writer: Some(writer),
      batch: R::new_batch(self.bsize),
      batch_size: self.bsize,
      batch_count: 0,
      row_count: 0,
    })
  }
}

impl <R> TableWriter<R> where R: TableRow {
  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
    let bld = TableWriterBuilder::new();
    bld.open(path)
  }

  fn write_batch(&mut self) -> Result<()> {
    debug!("writing batch");
    let cols = R::finish_batch(&mut self.batch);
    let batch = RecordBatch::try_new(self.schema.clone(), cols)?;
    let writer = self.writer.as_mut().ok_or(anyhow!("writer has been closed"))?;
    writer.write_object(batch)?;
    self.batch_count = 0;
    Ok(())
  }
}

impl <R> ObjectWriter<R> for TableWriter<R> where R: TableRow {
  fn write_object(&mut self, row: R) -> Result<()> {
    row.write_to_batch(&mut self.batch)?;
    self.batch_count += 1;
    self.row_count += 1;
    if self.batch_count >= self.batch_size {
      self.write_batch()?;
    }
    Ok(())
  }

  fn finish(mut self) -> Result<usize> {
    if self.batch_count > 0 {
      self.write_batch()?;
    }
    if let Some(writer) = self.writer.take() {
      info!("closing Parquet writer");
      writer.finish()?;
    } else {
      warn!("writer already closed");
    }
    Ok(self.row_count)
  }
}

impl <R> Drop for TableWriter<R> where R: TableRow {
  fn drop(&mut self) {
    if self.writer.is_some() {
      error!("Parquet table writer not closed");
    }
  }
}
