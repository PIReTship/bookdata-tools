//! Support for tables in Arrow
use arrow::array::*;
use arrow::datatypes::*;
use parquet::arrow::schema::arrow_to_parquet_schema;
use parquet::schema::types::{Type};

use anyhow::{Result};

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
}
