//! GoodReads review data model.
pub use serde::Deserialize;
use chrono::NaiveDateTime;

use crate::prelude::*;
use crate::arrow::*;
use crate::ids::index::IdIndex;
use crate::parsing::*;
use crate::parsing::dates::*;

const OUT_FILE: &'static str = "gr-reviews.parquet";

/// Review records we read from JSON.
#[derive(Deserialize)]
pub struct RawReview {
  pub user_id: String,
  pub book_id: String,
  pub review_id: String,
  pub rating: f32,
  pub review_text: String,
  pub n_votes: i32,
  pub date_added: String,
  pub date_updated: String,
  pub read_at: String,
  pub started_at: String,
}

/// Review records to write to the Parquet table.
#[derive(ParquetRecordWriter)]
pub struct ReviewRecord {
  pub rec_id: u32,
  pub review_id: i64,
  pub user_id: i32,
  pub book_id: i32,
  pub rating: Option<f32>,
  pub review: String,
  pub n_votes: i32,
  pub added: NaiveDateTime,
  pub updated: NaiveDateTime,
}

// Object writer to transform and write GoodReads reviews
pub struct ReviewWriter {
  writer: TableWriter<ReviewRecord>,
  users: IdIndex<Vec<u8>>,
  n_recs: u32,
}

impl ReviewWriter {
  // Open a new output
  pub fn open() -> Result<ReviewWriter> {
    let writer = TableWriter::open(OUT_FILE)?;
    Ok(ReviewWriter {
      writer,
      users: IdIndex::new(),
      n_recs: 0
    })
  }
}

impl DataSink for ReviewWriter {
  fn output_files(&self) -> Vec<PathBuf> {
    path_list(&[OUT_FILE])
  }
}

impl ObjectWriter<RawReview> for ReviewWriter {
  // Write a single interaction to the output
  fn write_object(&mut self, row: RawReview) -> Result<()> {
    self.n_recs += 1;
    let rec_id = self.n_recs;
    let user_key = hex::decode(row.user_id.as_bytes())?;
    let user_id = self.users.intern_owned(user_key);
    let book_id: i32 = row.book_id.parse()?;
    let (rev_hi, rev_lo) = decode_hex_i64_pair(&row.review_id)?;
    let review_id = rev_hi ^ rev_lo;

    self.writer.write_object(ReviewRecord {
      rec_id, review_id, user_id, book_id,
      review: row.review_text,
      rating: if row.rating > 0.0 {
        Some(row.rating)
      } else {
        None
      },
      n_votes: row.n_votes,
      added: parse_gr_date(&row.date_added)?,
      updated: parse_gr_date(&row.date_updated)?,
    })?;

    Ok(())
  }

  // Clean up and finalize output
  fn finish(self) -> Result<usize> {
    info!("wrote {} records for {} users, closing output", self.n_recs, self.users.len());
    self.writer.finish()
  }
}
