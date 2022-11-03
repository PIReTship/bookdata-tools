use std::io::Write;
use std::path::{Path, PathBuf};
use std::fs::{OpenOptions};
use std::mem::{replace};
use std::marker::PhantomData;

use log::*;
use anyhow::{Result, anyhow};
use arrow2::io::parquet::write::*;
use arrow2::array::{MutableArray, TryExtend, StructArray, Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::*;
use arrow2_convert::serialize::{ArrowSerialize};

use crate::io::object::{ObjectWriter, ThreadObjectWriter};
use crate::io::{DataSink};

const BATCH_SIZE: usize = 1024 * 1024;

/// Parquet table writer.
///
/// A table writer is an [ObjectWriter] for structs implementing [ArrowSerialize], that writes
/// them out to a Parquet file.
pub struct TableWriter<R: ArrowSerialize + Send + Sync + 'static> {
  _phantom: PhantomData<R>,
  writer: ThreadObjectWriter<Vec<R>>,
  out_path: Option<PathBuf>,
  batch: Vec<R>,
  batch_size: usize,
  row_count: usize
}

impl <R> TableWriter<R> where R: ArrowSerialize + Send + Sync + 'static, R::MutableArrayType: TryExtend<Option<R>> {
  /// Open a table writer for a path.
  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
    let path = path.as_ref();

    // extract struct schema
    let schema = match R::data_type() {
      DataType::Struct(fields) => {
        Schema {
          fields, metadata: Default::default()
        }
      },
      d => panic!("invalid data type {:?}", d)
    };

    let compression = CompressionOptions::Zstd(None);
    let options = WriteOptions { write_statistics: true, version: Version::V2, compression };

    info!("creating Parquet file {:?}", path);
    let file = OpenOptions::new().create(true).truncate(true).write(true).open(path)?;
    let writer = FileWriter::try_new(file, schema, options)?;
    let writer = ThreadObjectWriter::new(writer);
    let writer = writer.with_transform(vec_to_chunk);
    let writer = ThreadObjectWriter::new(writer);
    let out_path = Some(path.to_path_buf());
    Ok(TableWriter {
      _phantom: PhantomData,
      writer,
      out_path,
      batch: Vec::with_capacity(BATCH_SIZE),
      batch_size: BATCH_SIZE,
      row_count: 0,
    })
  }

  fn write_batch(&mut self) -> Result<()> {
    if self.batch.is_empty() {
      return Ok(());
    }

    let batch = replace(&mut self.batch, Vec::with_capacity(self.batch_size));
    self.writer.write_object(batch)?;

    Ok(())
  }
}

impl <R> DataSink for TableWriter<R> where R: ArrowSerialize + Send + Sync + 'static {
  fn output_files(&self) -> Vec<PathBuf> {
    match &self.out_path {
      None => Vec::new(),
      Some(p) => vec![p.clone()]
    }
  }
}

impl <R> ObjectWriter<R> for TableWriter<R> where R: ArrowSerialize + Send + Sync + 'static, R::MutableArrayType: TryExtend<Option<R>> {
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
    self.writer.finish()?;
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

/// Convert a vector of records to a chunk.
pub fn vec_to_chunk<R>(vec: Vec<R>) -> Result<Chunk<Box<dyn Array>>>
where R: ArrowSerialize, R::MutableArrayType: TryExtend<Option<R>>
{
  let mut array = R::new_array();
  array.reserve(vec.len());
  array.try_extend(vec.into_iter().map(Some))?;

  // get the struct array to chunkify
  let array = array.as_box();
  let sa = array.as_any().downcast_ref::<StructArray>();
  let sa = sa.ok_or_else(|| anyhow!("invalid array type (not a structure)"))?;
  let (_fields, cols, validity) = sa.to_owned().into_data();
  if validity.is_some() {
    return Err(anyhow!("structure arrays with validity not supported"))
  }
  let chunk = Chunk::new(cols);
  Ok(chunk)
}

impl <W> ObjectWriter<Chunk<Box<dyn Array + 'static>>> for FileWriter<W> where W: Write {
  fn write_object(&mut self, chunk: Chunk<Box<dyn Array + 'static>>) -> Result<()> {
    let schema = self.schema();
    let encodings: Vec<_> = schema.fields.iter().map(|f| transverse(&f.data_type, |_| Encoding::Plain)).collect();
    let options = self.options();
    let chunks = vec![Ok(chunk)];
    let groups = RowGroupIterator::try_new(chunks.into_iter(), &schema, options, encodings)?;
    for group in groups {
      self.write(group?)?;
    }
    Ok(())
  }

  fn finish(mut self) -> Result<usize> {
    self.end(None)?;
    Ok(0)
  }
}
