//! Structs defining Amazon data sets.
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

/// A rating as described in a source CSV file.
#[derive(Serialize, Deserialize)]
pub struct SourceRating {
    pub user: String,
    pub asin: String,
    pub rating: f32,
    pub timestamp: i64,
}

/// A review as it is described in a source JSON file.
#[derive(Serialize, Deserialize)]
pub struct SourceReview {
    #[serde(rename = "reviewerID")]
    pub user: String,
    pub asin: String,
    #[serde(rename = "overall")]
    pub rating: f32,
    #[serde(rename = "unixReviewTime")]
    pub timestamp: i64,
    pub summary: Option<String>,
    #[serde(rename = "reviewText")]
    pub text: Option<String>,
    pub verified: bool,
}

/// Structure for scanned ratings.
///
/// This data structure is serialized to `ratings.parquet` in the Amazon directories.
#[derive(ParquetRecordWriter, Serialize, Deserialize)]
pub struct RatingRow {
    pub user_id: i32,
    pub asin: String,
    pub rating: f32,
    pub timestamp: i64,
}

/// Structure for scanned reviews.
///
/// This data structure is serialized to `reviews.parquet` in the Amazon directories.
#[derive(ParquetRecordWriter, Serialize, Deserialize)]
pub struct ReviewRow {
    pub user_id: i32,
    pub asin: String,
    pub rating: f32,
    pub timestamp: i64,
    pub summary: String,
    pub text: String,
}
