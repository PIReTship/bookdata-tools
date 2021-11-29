pub use serde::Deserialize;
use chrono::{DateTime, FixedOffset};

use crate::prelude::*;
use crate::arrow::*;
use crate::ids::index::IdIndex;
use crate::parsing::*;
use crate::parsing::dates::*;

const OUT_FILE: &'static str = "gr-interactions.parquet";

// the records we read from JSON
#[derive(Deserialize)]
pub struct RawInteraction {
  pub user_id: String,
  pub book_id: String,
  // review_id: String,
  #[serde(rename="isRead")]
  pub is_read: bool,
  pub rating: f32,
  pub date_added: String,
  pub date_updated: String,
  pub read_at: String,
  pub started_at: String,
}

// the records we're actually going to write to the table
#[derive(TableRow)]
pub struct IntRecord {
  pub rec_id: u32,
  pub user_id: u32,
  pub book_id: i32,
  pub is_read: u8,
  pub rating: Option<f32>,
  pub added: DateTime<FixedOffset>,
  pub updated: DateTime<FixedOffset>,
  pub read_started: Option<DateTime<FixedOffset>>,
  pub read_finished: Option<DateTime<FixedOffset>>,
}

// Object writer to transform and write GoodReads interactions
pub struct IntWriter {
  writer: TableWriter<IntRecord>,
  users: IdIndex<Vec<u8>>,
  n_recs: u32,
}

impl IntWriter {
  // Open a new output
  pub fn open() -> Result<IntWriter> {
    let writer = TableWriter::open(OUT_FILE)?;
    Ok(IntWriter {
      writer,
      users: IdIndex::new(),
      n_recs: 0
    })
  }
}

impl DataSink for IntWriter {
  fn output_files(&self) -> Vec<PathBuf> {
    path_list(&[OUT_FILE])
  }
}

impl ObjectWriter<RawInteraction> for IntWriter {
  // Write a single interaction to the output
  fn write_object(&mut self, row: RawInteraction) -> Result<()> {
    self.n_recs += 1;
    let rec_id = self.n_recs;
    let user_key = hex::decode(row.user_id.as_bytes())?;
    let user_id = self.users.intern_owned(user_key);
    let book_id: i32 = row.book_id.parse()?;

    self.writer.write_object(IntRecord {
      rec_id, user_id, book_id,
      is_read: row.is_read as u8,
      rating: if row.rating > 0.0 {
        Some(row.rating)
      } else {
        None
      },
      added: parse_gr_date(&row.date_added)?,
      updated: parse_gr_date(&row.date_updated)?,
      read_started: trim_opt(&row.started_at).map(parse_gr_date).transpose()?,
      read_finished: trim_opt(&row.read_at).map(parse_gr_date).transpose()?,
    })?;

    Ok(())
  }

  // Clean up and finalize output
  fn finish(self) -> Result<usize> {
    info!("wrote {} records for {} users, closing output", self.n_recs, self.users.len());
    self.writer.finish()
  }
}
