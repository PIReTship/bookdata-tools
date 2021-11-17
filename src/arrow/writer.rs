use std::path::Path;
use std::fs::OpenOptions;
use std::sync::Arc;
use std::marker::PhantomData;

use log::*;
use arrow::datatypes::{Schema, SchemaRef, Field};
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use parquet::schema::types::ColumnPath;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::file::writer::ParquetWriter;
use parquet::arrow::ArrowWriter;
use anyhow::{Result, anyhow};

use crate::io::object::{ObjectWriter, ThreadWriter};
use super::table::TableRow;

const BATCH_SIZE: usize = 1000000;

/// Parquet table writer
pub struct TableWriter<R: TableRow> {
  schema: SchemaRef,
  select: Vec<usize>,
  writer: Option<ThreadWriter<RecordBatch>>,
  batch: R::Batch,
  batch_size: usize,
  batch_count: usize,
  row_count: usize
}

/// Builder for Parquet table writers.
pub struct TableWriterBuilder<R: TableRow> {
  phantom: PhantomData<R>,
  schema: SchemaRef,
  select: Vec<usize>,
  props: WriterPropertiesBuilder,
  bsize: usize,
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
    let schema = Arc::new(R::schema());
    let select = (0..schema.fields().len()).collect::<Vec<usize>>();
    let props = WriterProperties::builder();
    let props = props.set_dictionary_enabled(false);
    let props = props.set_compression(Compression::ZSTD);
    let props = R::adjust_writer_props(props);
    TableWriterBuilder {
      phantom: PhantomData,
      schema, select, props,
      bsize: BATCH_SIZE,
    }
  }

  pub fn rename(mut self, orig: &str, tgt: &str) -> TableWriterBuilder<R> {
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

  pub fn batch_size(mut self, size: usize) -> TableWriterBuilder<R> {
    self.bsize = size;
    self
  }

  /// Write a subset of the columns.
  ///
  /// This filters the resulting writer. The columns are still assembled in memory (for
  /// implementation convenience), but will not be written the output file.
  pub fn project(mut self, cols: &[&str]) -> TableWriterBuilder<R> {
    let mut f2 = Vec::with_capacity(cols.len());
    let mut s2 = Vec::with_capacity(cols.len());
    for c in cols {
      let idx = self.schema.index_of(c).expect("cannot project to unknown field");
      s2.push(self.select[idx]);
      f2.push(self.schema.field(idx).clone());
    }
    self.schema = Arc::new(Schema::new(f2));
    self.select = s2;
    self
  }

  pub fn column_encoding<C: Into<ColumnPath>>(mut self, col: C, enc: Encoding) -> TableWriterBuilder<R> {
    self.props = self.props.set_column_encoding(col.into(), enc);
    self
  }

  pub fn open<P: AsRef<Path>>(self, path: P) -> Result<TableWriter<R>> {
    let file = OpenOptions::new().create(true).truncate(true).write(true).open(path)?;
    let props = self.props.build();
    let writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;
    let writer = ThreadWriter::new(writer);
    Ok(TableWriter {
      schema: self.schema,
      select: self.select,
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
    let proj_cols: Vec<ArrayRef> = self.select.iter().map(|i| cols[*i].clone()).collect();
    let batch = RecordBatch::try_new(self.schema.clone(), proj_cols)?;
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
