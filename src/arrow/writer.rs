use std::path::{Path, PathBuf};
use std::fs::{OpenOptions, File};
use std::mem::drop;
use std::sync::Arc;
use std::marker::PhantomData;

use parquet::record::RecordWriter;
use parquet::schema::types::{ColumnPath, Type};
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use anyhow::Result;

use crate::io::object::{ObjectWriter};
use crate::io::DataSink;

const BATCH_SIZE: usize = 1024 * 1024;

/// Parquet table writer.
///
/// A table writer is an [ObjectWriter] for structs implementing [RecordWriter], that writes
/// them out to a Parquet file.
pub struct TableWriter<R> {
  _phantom: PhantomData<R>,
  writer: SerializedFileWriter<File>,
  out_path: Option<PathBuf>,
  batch: Vec<R>,
  batch_size: usize,
  row_count: usize
}

/// Builder for Parquet table writers.
pub struct TableWriterBuilder<R> {
  _phantom: PhantomData<R>,
  schema: Arc<Type>,
  props: WriterPropertiesBuilder,
  bsize: usize,
}

impl <R> TableWriterBuilder<R> where R: 'static, for<'a> &'a [R]: RecordWriter<R> {
  pub fn new() -> Result<TableWriterBuilder<R>> {
    let v: Vec<R> = Vec::new();
    let vr: &[R] = &v;
    let schema = vr.schema()?;
    let props = WriterProperties::builder();
    let props = props.set_dictionary_enabled(false);
    let props = props.set_compression(Compression::ZSTD);
    Ok(TableWriterBuilder {
      _phantom: PhantomData,
      schema, props,
      bsize: BATCH_SIZE,
    })
  }

  pub fn rename(mut self, orig: &str, tgt: &str) -> TableWriterBuilder<R> {
    let fields = self.schema.get_fields();
    let mut nf = Vec::new();

    for field in fields {
      if field.name() == orig {
        if let Type::PrimitiveType {
          basic_info, physical_type,
          type_length, scale, precision,
        } = field.as_ref() {
          let t = Type::primitive_type_builder(tgt, physical_type.clone())
            .with_repetition(basic_info.repetition())
            .with_converted_type(basic_info.converted_type())
            .with_logical_type(basic_info.logical_type())
            .with_length(*type_length)
            .with_scale(*scale)
            .with_precision(*precision)
            .build().expect("type reconstruction failure");
          nf.push(Arc::new(t));
        } else {
          panic!("column type is not primitive");
        }
      } else {
        nf.push(field.clone());
      }
    }

    let g = Type::group_type_builder(self.schema.name())
      .with_fields(&mut nf)
      .build().expect("type reconstruction failure");

    self.schema = Arc::new(g);
    self
  }

  /// Set the batch size for a table writer builder.
  #[allow(dead_code)]
  pub fn batch_size(mut self, size: usize) -> TableWriterBuilder<R> {
    self.bsize = size;
    self
  }

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
    let schema = self.schema.clone();
    let writer = SerializedFileWriter::new(file, schema, props)?;
    let out_path = Some(path.to_path_buf());
    Ok(TableWriter {
      _phantom: PhantomData,
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
    br.write_to_row_group(&mut rg)?;
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
