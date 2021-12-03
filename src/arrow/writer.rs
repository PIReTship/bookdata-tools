use std::path::{Path, PathBuf};
use std::fs::{OpenOptions, File};
use std::mem::drop;
use std::sync::Arc;
use std::marker::PhantomData;

use parquet::record::RecordWriter;
use parquet::schema::types::{ColumnPath, SchemaDescriptor};
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use anyhow::Result;

use crate::io::object::{ObjectWriter};
use crate::io::DataSink;

const BATCH_SIZE: usize = 1000000;

/// Parquet table writer.
///
/// A table writer is an [ObjectWriter] for structs implementing [RecordWriter], that writes
/// them out to a Parquet file.
pub struct TableWriter<R> {
  _phantom: PhantomData<R>,
  schema: SchemaDescriptor,
  writer: SerializedFileWriter<File>,
  out_path: Option<PathBuf>,
  batch: Vec<R>,
  batch_size: usize,
  row_count: usize
}

/// Builder for Parquet table writers.
pub struct TableWriterBuilder<R> {
  _phantom: PhantomData<R>,
  schema: SchemaDescriptor,
  select: Vec<usize>,
  props: WriterPropertiesBuilder,
  bsize: usize,
}

impl <R> TableWriterBuilder<R> where R: 'static, for<'a> &'a [R]: RecordWriter<R> {
  pub fn new() -> Result<TableWriterBuilder<R>> {
    let v: Vec<R> = Vec::new();
    let vr: &[R] = &v;
    let schema = vr.schema()?;
    let schema = SchemaDescriptor::new(schema);
    let select = (0..schema.num_columns()).collect::<Vec<usize>>();
    let props = WriterProperties::builder();
    let props = props.set_dictionary_enabled(false);
    let props = props.set_compression(Compression::ZSTD);
    Ok(TableWriterBuilder {
      _phantom: PhantomData,
      schema, select, props,
      bsize: BATCH_SIZE,
    })
  }

  pub fn rename(mut self, orig: &str, tgt: &str) -> TableWriterBuilder<R> {
    panic!("does not work");
    // let fields = self.schema.columns();
    // let mut f2 = Vec::with_capacity(fields.len());
    // for f in fields {
    //   if f.name() == orig {
    //     f2.push(Field::new(tgt, f.data_type().clone(), f.is_nullable()))
    //   } else {
    //     f2.push(f.clone())
    //   }
    // }
    // self.schema = Arc::new(Schema::new(f2));
    // self
  }

  /// Set the batch size for a table writer builder.
  #[allow(dead_code)]
  pub fn batch_size(mut self, size: usize) -> TableWriterBuilder<R> {
    self.bsize = size;
    self
  }

  /// Write a subset of the columns.
  ///
  /// This filters the resulting writer. The columns are still assembled in memory (for
  /// implementation convenience), but will not be written the output file.
  // pub fn project(mut self, cols: &[&str]) -> TableWriterBuilder<R> {
  //   let mut f2 = Vec::with_capacity(cols.len());
  //   let mut s2 = Vec::with_capacity(cols.len());
  //   for c in cols {
  //     let idx = self.schema.index_of(c).expect("cannot project to unknown field");
  //     s2.push(self.select[idx]);
  //     f2.push(self.schema.field(idx).clone());
  //   }
  //   self.schema = Arc::new(Schema::new(f2));
  //   self.select = s2;
  //   self
  // }

  /// Set the encoding for a particular column.
  #[allow(dead_code)]
  pub fn column_encoding<C: Into<ColumnPath>>(mut self, col: C, enc: Encoding) -> TableWriterBuilder<R> {
    self.props = self.props.set_column_encoding(col.into(), enc);
    self
  }

  /// Open the configured table writer.
  pub fn open<P: AsRef<Path>>(self, path: P) -> Result<TableWriter<R>> {
    let path = path.as_ref();
    let file = OpenOptions::new().create(true).truncate(true).write(true).open(path)?;
    let props = self.props.build();
    let props = Arc::new(props);
    let schema = self.schema.root_schema_ptr();
    let writer = SerializedFileWriter::new(file, schema, props)?;
    let out_path = Some(path.to_path_buf());
    Ok(TableWriter {
      _phantom: PhantomData,
      schema: self.schema,
      writer,
      out_path,
      batch: Vec::with_capacity(self.bsize),
      batch_size: self.bsize,
      row_count: 0,
    })
  }
}

impl <R> TableWriter<R> where R: 'static, for<'a> &'a [R]: RecordWriter<R> {
  /// Open a table writer for a path.
  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
    let bld = TableWriterBuilder::new()?;
    bld.open(path)
  }

  fn write_batch(&mut self) -> Result<()> {
    if self.batch.is_empty() {
      return Ok(());
    }

    let br: &[R] = &self.batch;
    let mut rg = self.writer.next_row_group()?;
    br.write_to_row_group(&mut rg);
    self.writer.close_row_group(rg)?;

    drop(br);
    self.batch.truncate(0);
    Ok(())
  }
}

impl <R> DataSink for TableWriter<R> {
  fn output_files(&self) -> Vec<PathBuf> {
    match &self.out_path {
      None => Vec::new(),
      Some(p) => vec![p.clone()]
    }
  }
}

impl <R> ObjectWriter<R> for TableWriter<R> where R: 'static,  for<'a> &'a [R]: RecordWriter<R> {
  fn write_object(&mut self, row: R) -> Result<()> {
    self.batch.push(row);
    if self.batch.len() >= self.batch_size {
      self.write_batch()?;
    }
    self.row_count += 1;

    Ok(())
  }

  fn finish(mut self) -> Result<usize> {
    if !self.batch.is_empty() {
      self.write_batch()?;
    }
    self.writer.close()?;
    // if let Some(writer) = self.writer.take() {
    //   info!("closing Parquet writer");
    //   writer.finish()?;
    // } else {
    //   warn!("writer already closed");
    // }
    Ok(self.row_count)
  }
}

// impl <W> Drop for TableWriter<W> where W: RecordWriter<W> {
//   fn drop(&mut self) {
//     if self.writer.is_some() {
//       error!("Parquet table writer not closed");
//     }
//   }
// }
