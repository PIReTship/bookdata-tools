//! Support for tables in Arrow
use std::fs;
use std::marker::PhantomData;
use std::path::Path;

use arrow::array::*;
use arrow::datatypes::*;
use parquet::arrow::schema::arrow_to_parquet_schema;
use parquet::schema::types::{Type};
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::record::RowAccessor;
use parquet::record::reader::RowIter;

use anyhow::Result;

pub use bd_macros::TableRow;

/// Trait for serializing records into Arrow/Parquet tables.
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
pub trait TableRow where Self: Sized {
  type Batch;

  /// Get the schema
  fn schema() -> Schema;

  /// Get a Parquet schema.
  fn pq_schema() -> Type {
    let schema = Self::schema();
    let schema = arrow_to_parquet_schema(&schema).expect("cannot make parquet schema");
    schema.root_schema().clone()
  }

  /// Create a new Batch Builder
  fn new_batch(cap: usize) -> Self::Batch;

  /// Get a batch's arrays and reset the builder.
  fn finish_batch(batch: &mut Self::Batch) -> Vec<ArrayRef>;

  /// Add a record to a batch builder
  fn write_to_batch(&self, batch: &mut Self::Batch) -> Result<()>;

  /// Extract a row from a row accessor.
  fn from_pq_row<R: RowAccessor>(row: &R) -> Result<Self>;

  /// Iterate rows from a Parquet file.
  fn scan_parquet<'a, P: AsRef<Path>>(path: P) -> Result<ScanIter<'a, Self>> {
    let schema = Self::pq_schema();
    let file = fs::File::open(path)?;
    // we use an unsafe pointer to get around an annoying lifetime issue
    let reader = Box::into_raw(Box::new(SerializedFileReader::new(file)?));
    let iter = unsafe {
      RowIter::from_file(Some(schema), &*reader)?
    };
    Ok(ScanIter {
      reader, iter, _m: PhantomData
    })
  }
}

pub struct ScanIter<'a, T: TableRow> {
  reader: *mut SerializedFileReader<fs::File>,
  iter: RowIter<'a>,
  _m: PhantomData<T>
}

impl <'a, T: TableRow> Drop for ScanIter<'a, T> {
  fn drop(&mut self) {
    // free the reader
    unsafe {
      let _rbox = Box::from_raw(self.reader);
    }
  }
}

impl <'a, T: TableRow> Iterator for ScanIter<'a, T> {
  type Item = Result<T>;

  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().as_ref().map(T::from_pq_row)
  }
}
