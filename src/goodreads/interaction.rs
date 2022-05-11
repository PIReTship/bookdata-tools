use hashbrown::HashSet;
use serde::Deserialize;

use crate::prelude::*;
use crate::arrow::*;
use crate::ids::index::IdIndex;
use crate::parsing::*;
use crate::parsing::dates::*;

pub const OUT_FILE: &'static str = "gr-interactions.parquet";
pub const USER_FILE: &'static str = "gr-users.parquet";
pub const UID_COL: &'static str = "user";
pub const UHASH_COL: &'static str = "user_hash";

// the records we read from JSON
#[derive(Deserialize)]
pub struct RawInteraction {
  pub user_id: String,
  pub book_id: String,
  pub review_id: String,
  #[serde(rename="isRead")]
  pub is_read: bool,
  pub rating: f32,
  pub date_added: String,
  pub date_updated: String,
  pub read_at: String,
  pub started_at: String,
}

/// GoodReads interaction records as actually written to the table.
///
/// This struct is written to `gr-interactions.parquet` and records actual interaction data.
/// Timestamps are UNIX timestamps recorded as 64-bit integers; they do not use a Parquet
/// timestamp time, due to out-of-range values causing problems when loaded into Python.
#[derive(ParquetRecordWriter)]
pub struct IntRecord {
  pub rec_id: u32,
  /// The review ID.
  ///
  /// This is derived from the hexadecimal review ID by interpreting the hexadecimal-encoded
  /// review ID from the source data as two big-endian i64s and XORing them.  The import
  /// process checks that this does not result in duplicate review IDs, and emits warnings
  /// if any are encountered.
  pub review_id: i64,
  pub user_id: i32,
  pub book_id: i32,
  pub is_read: u8,
  pub rating: Option<f32>,
  pub added: f32,
  pub updated: f32,
  pub read_started: Option<f32>,
  pub read_finished: Option<f32>,
}

// Object writer to transform and write GoodReads interactions
pub struct IntWriter {
  writer: TableWriter<IntRecord>,
  users: IdIndex<String>,
  review_ids: HashSet<i64>,
  n_recs: u32,
}

impl IntWriter {
  // Open a new output
  pub fn open() -> Result<IntWriter> {
    let writer = TableWriter::open(OUT_FILE)?;
    Ok(IntWriter {
      writer,
      users: IdIndex::new(),
      review_ids: HashSet::new(),
      n_recs: 0,
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
    let user_id = self.users.intern_owned(row.user_id)?;
    let book_id: i32 = row.book_id.parse()?;
    let (rev_hi, rev_lo) = decode_hex_i64_pair(&row.review_id)?;
    let review_id = rev_hi ^ rev_lo;
    if !self.review_ids.insert(review_id) {
      warn!("review id {} duplicated ({})", review_id, row.review_id);
    }

    self.writer.write_object(IntRecord {
      rec_id, review_id, user_id, book_id,
      is_read: row.is_read as u8,
      rating: if row.rating > 0.0 {
        Some(row.rating)
      } else {
        None
      },
      added: parse_gr_date(&row.date_added).map(check_ts("added", 2005))?,
      updated: parse_gr_date(&row.date_updated).map(check_ts("updated", 2005))?,
      read_started: trim_opt(&row.started_at).map(parse_gr_date).transpose()?.map(check_ts("started", 1900)),
      read_finished: trim_opt(&row.read_at).map(parse_gr_date).transpose()?.map(check_ts("finished", 1900)),
    })?;

    Ok(())
  }

  // Clean up and finalize output
  fn finish(self) -> Result<usize> {
    info!("wrote {} records for {} users, closing output", self.n_recs, self.users.len());
    let res = self.writer.finish()?;
    info!("saving {} users", self.users.len());
    self.users.save(USER_FILE, UID_COL, UHASH_COL)?;
    Ok(res)
  }
}
