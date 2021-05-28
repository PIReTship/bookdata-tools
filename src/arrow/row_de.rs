//! Code to deserialize from Arrow with serde.
//!
//! This supports deserializing from record batches, or from vectors or
//! streams of record batches, into a row type that implements
//! [serde::Deserialize].
use std::path::Path;
use std::fs::File;
use std::fmt::Display;
use std::sync::Arc;
use std::pin::Pin;
use std::marker::PhantomData;

use futures::task::{Context,Poll};
use futures::{Stream, TryStream, TryStreamExt};
use fallible_iterator::FallibleIterator;
use thiserror::Error;
use anyhow::Result;

use arrow::error::ArrowError;
use arrow::datatypes::*;
use arrow::array::*;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use parquet::file::reader::SerializedFileReader;
use parquet::arrow::arrow_reader::{ArrowReader, ParquetFileArrowReader, ParquetRecordBatchReader};
use parquet::errors::ParquetError;

use paste::paste;

use serde::{forward_to_deserialize_any};
use serde::de::{
  self,
  Deserialize,
  Deserializer,
  Visitor,
  MapAccess,
  DeserializeSeed,
};

/// Error raised by row deserialization
#[derive(Error, Debug)]
pub enum RowError {
  #[error("Serde decode error {0}")]
  SerdeError(String),
  #[error("Parquet error {0}")]
  Parquet(#[from] ParquetError),
  #[error("Arrow error {0}")]
  Arrow(#[from] ArrowError),
  #[error("error casting array type")]
  BadArrayCast,
  #[error("data type not supported")]
  UnsupportedType(DataType),
}

impl de::Error for RowError {
  fn custom<T: Display>(msg: T) -> RowError {
    RowError::SerdeError(format!("{}", msg))
  }
}

/// Deserialize a record batch
pub struct RecordBatchDeserializer {
  schema: SchemaRef,
  batch: RecordBatch,
  ncols: usize,
  nrows: usize
}

/// Iterator over deserialized records from a record batch
pub struct BatchRecordIter<R> {
  rbd: RecordBatchDeserializer,
  row: usize,
  _ph: PhantomData<R>
}

/// Iterator over deserialized records from an iterator of records.
pub struct RecordIter<R, B: RecordBatchReader> {
  rb_iter: B,
  cur_batch: Option<BatchRecordIter<R>>,
}

/// Deserialization state for one row
struct RowState<'a> {
  rbd: &'a RecordBatchDeserializer,
  row: usize,
  col: usize,
}

/// A column name for deserialization
struct ColName<'a>(&'a str);

/// A column value for deserialization
struct ColValue<'a> {
  array: &'a dyn Array,
  row: usize
}

pub fn scan_parquet_file<'de, R: Deserialize<'de>, P: AsRef<Path>>(path: P) -> Result<RecordIter<R, ParquetRecordBatchReader>> {
  let file = File::open(path)?;
  let read = Arc::new(SerializedFileReader::new(file)?);
  let mut read = ParquetFileArrowReader::new(read);
  let rb_iter = read.get_record_reader(1_000_000)?;
  Ok(RecordIter {
    rb_iter, cur_batch: None
  })
}

impl <'a> RecordBatchDeserializer {
  /// Deserialize rows from a stream of record batches.
  pub fn for_stream<'de, R, E, S>(stream: S) -> impl Stream<Item=Result<R>>
  where R: Deserialize<'de>,
        E: std::error::Error + Send + Sync + 'static,
        S: TryStream<Ok=RecordBatch, Error=E>
  {
    stream.map_ok(|batch| {
      RecordBatchDeserializer::new(batch).iter().err_into()
      // <BatchRecordIter<R> as TryStreamExt>::map_err(batch, anyhow::Error::from)
    }).try_flatten()
  }

  pub fn new(batch: RecordBatch) -> RecordBatchDeserializer {
    let schema = batch.schema();
    let ncols = batch.columns().len();
    let nrows = batch.num_rows();
    RecordBatchDeserializer {
      schema, batch, ncols, nrows,
    }
  }

  pub fn iter<'de, R: Deserialize<'de>>(self) -> BatchRecordIter<R> {
    BatchRecordIter {
      rbd: self,
      row: 0,
      _ph: PhantomData
    }
  }
}

impl <'de, R: Deserialize<'de>, B: RecordBatchReader> FallibleIterator for RecordIter<R, B> {
  type Item = R;
  type Error = RowError;

  fn next(&mut self) -> Result<Option<R>, RowError> {
    if let Some(ref mut bi) = self.cur_batch {
      let n = bi.next()?;
      if n.is_some() {
        Ok(n)
      } else {
        // this iterator is exhausted, reset and try again
        self.cur_batch = None;
        self.next()
      }
    } else {
      // no batch iterator -- look for another
      match self.rb_iter.next() {
        None => Ok(None),  // we are done
        Some(Ok(batch)) => {
          // we have a new iterator
          let rbd = RecordBatchDeserializer::new(batch);
          self.cur_batch = Some(rbd.iter());
          self.next()
        },
        Some(Err(e)) => {
          // we failed to read a batch
          Err(e.into())
        }
      }
    }
  }
}

impl <'de, R: Deserialize<'de>> Unpin for BatchRecordIter<R> {}

impl <'de, R: Deserialize<'de>> BatchRecordIter<R> {
  fn advance(self: Pin<&mut Self>) {
    self.get_mut().row += 1;
  }
}

impl <'de, R: Deserialize<'de>> FallibleIterator for BatchRecordIter<R> {
  type Item = R;
  type Error = RowError;

  fn next(&mut self) -> Result<Option<R>, RowError> {
    if self.row >= self.rbd.nrows {
      Ok(None)
    } else {
      let rs = RowState {
        rbd: &self.rbd, row: self.row, col: 0
      };
      self.row += 1;
      Ok(Some(R::deserialize(rs)?))
    }
  }
}

impl <'de, R: Deserialize<'de>> Stream for BatchRecordIter<R> {
  type Item = Result<R, RowError>;

  fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let row = self.row;
    if row >= self.rbd.nrows {
      Poll::Ready(None)
    } else {
      // annoying indirection to deal with pinning
      self.as_mut().advance();
      let rs = RowState {
        rbd: &self.rbd, row: row, col: 0
      };
      Poll::Ready(Some(R::deserialize(rs).map_err(|e| e.into())))
    }
  }
}

impl <'a, 'de> Deserializer<'de> for RowState<'a> {
  type Error = RowError;

  fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
    visitor.visit_map(self)
  }

  fn deserialize_struct<V: Visitor<'de>>(self, _name: &'static str, _fields: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error> {
    // TODO leverage the expected field names for projection
    visitor.visit_map(self)
  }

  forward_to_deserialize_any! {
    bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
    bytes byte_buf option unit unit_struct newtype_struct seq tuple
    tuple_struct map enum identifier ignored_any
  }
}

impl <'a, 'de> MapAccess<'de> for RowState<'a> {
  type Error = RowError;

  fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
  where K: DeserializeSeed<'de> {
    if self.col >= self.rbd.ncols {
      Ok(None)
    } else {
      let i = self.col;
      self.col += 1;
      let name = self.rbd.schema.fields()[i].name();
      seed.deserialize(ColName(name.as_ref())).map(Some)
    }
  }

  fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
  where V: DeserializeSeed<'de> {
    let i = self.col - 1;
    let columns = self.rbd.batch.columns();
    seed.deserialize(ColValue {
      array: columns[i].as_ref(),
      row: self.row
    })
  }
}

impl <'a, 'de> Deserializer<'de> for ColName<'a> {
  type Error = RowError;

  fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
    visitor.visit_str(self.0)
  }

  forward_to_deserialize_any! {
    bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
    bytes byte_buf option unit unit_struct newtype_struct seq tuple
    tuple_struct map enum struct identifier ignored_any
  }
}

// macro for creating a hinted deserialize method
macro_rules! hinted_deserialize {
  ($t:ty, $dt:ident) => {
    paste! {
      fn [<deserialize_ $t>]<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.array.data_type() {
          DataType::$dt => self.[<visit_ $t>](visitor),
          _ => self.deserialize_any(visitor)
        }
      }
    }
  };
}

impl <'a, 'de> Deserializer<'de> for ColValue<'a> {
  type Error = RowError;

  fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
    match self.array.data_type() {
      DataType::Null => visitor.visit_none(),
      DataType::Int8 => self.visit_i8(visitor),
      DataType::Int16 => self.visit_i16(visitor),
      DataType::Int32 => self.visit_i32(visitor),
      DataType::Int64 => self.visit_i64(visitor),
      DataType::UInt8 => self.visit_u8(visitor),
      DataType::UInt16 => self.visit_u16(visitor),
      DataType::UInt32 => self.visit_u32(visitor),
      DataType::UInt64 => self.visit_u64(visitor),
      DataType::Float32 => self.visit_f32(visitor),
      DataType::Float64 => self.visit_f64(visitor),
      DataType::Utf8 => self.visit_utf8(visitor),
      t => Err(RowError::UnsupportedType(t.clone()))
    }
  }

  hinted_deserialize!(i8, Int8);
  hinted_deserialize!(i16, Int16);
  hinted_deserialize!(i32, Int32);
  hinted_deserialize!(i64, Int64);
  hinted_deserialize!(u8, UInt8);
  hinted_deserialize!(u16, UInt16);
  hinted_deserialize!(u32, UInt32);
  hinted_deserialize!(u64, UInt64);
  hinted_deserialize!(f32, Float32);
  hinted_deserialize!(f64, Float64);

  fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
    // if expecting an option, then use option wrappers
    if self.array.is_valid(self.row) {
      visitor.visit_some(self)
    } else {
      visitor.visit_none()
    }
  }

  forward_to_deserialize_any! {
    // i16 i32 i64 u8 u16 u32 u64 f32 f64
    bool i128 u128 char str string
    bytes byte_buf unit unit_struct newtype_struct seq tuple
    tuple_struct map enum struct identifier ignored_any
  }
}

// A little macro to define primitive visitor delegates
macro_rules! primitive_visitor {
  ($name:ident, $atype:ty) => {
    fn $name<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, RowError> {
      let arr = self.array.as_any().downcast_ref::<$atype>().ok_or(RowError::BadArrayCast)?;
      if arr.is_valid(self.row) {
        visitor.$name(arr.value(self.row))
      } else {
        visitor.visit_none()
      }
    }
  };
}

impl <'a, 'de> ColValue<'a> {
  primitive_visitor!(visit_i8, Int8Array);
  primitive_visitor!(visit_i16, Int16Array);
  primitive_visitor!(visit_i32, Int32Array);
  primitive_visitor!(visit_i64, Int64Array);
  primitive_visitor!(visit_u8, UInt8Array);
  primitive_visitor!(visit_u16, UInt16Array);
  primitive_visitor!(visit_u32, UInt32Array);
  primitive_visitor!(visit_u64, UInt64Array);
  primitive_visitor!(visit_f32, Float32Array);
  primitive_visitor!(visit_f64, Float64Array);

  fn visit_utf8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, RowError> {
    let arr = self.array.as_any().downcast_ref::<StringArray>().ok_or(RowError::BadArrayCast)?;
    if arr.is_valid(self.row) {
      visitor.visit_str(arr.value(self.row))
    } else {
      visitor.visit_none()
    }
  }
}
