//! Structs defining Amazon data sets.
use serde::{Serialize, Deserialize};
use crate::arrow::*;

/// A rating as described in a source CSV file.
#[derive(Serialize, Deserialize)]
pub struct SourceRating {
  pub user: String,
  pub asin: String,
  pub rating: f32,
  pub timestamp: i64,
}

/// Structure for scanned ratings.
///
/// This data structure is serialized to `ratings.parquet` in the Amazon directories.
#[derive(ArrowField, Serialize, Deserialize)]
pub struct RatingRow {
  pub user: i32,
  pub asin: String,
  pub rating: f32,
  pub timestamp: i64,
}
