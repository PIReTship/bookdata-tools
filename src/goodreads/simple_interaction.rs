//! Simple interaction records from the publicly-available CSV files.
use hashbrown::HashMap;
use serde::Deserialize;

use crate::io::open_progress;
use crate::prelude::*;
use crate::arrow::*;
use crate::util::logging::data_progress;

pub const OUT_FILE: &str = "gr-simple-interactions.parquet";

/// Interaction records we read from CSV.
#[derive(Deserialize)]
pub struct RawInteraction {
  pub user_id: i32,
  pub book_id: i32,
  pub is_read: u8,
  pub rating: f32,
  pub is_reviewed: u8,
}

/// Book ID map record from CSV.
#[derive(Deserialize, Debug)]
pub struct RawBookMap {
  pub book_id_csv: i32,
  pub book_id: i32,
}

/// GoodReads interaction records as actually written to the table.
///
/// This struct is written to `gr-simple-interactions.parquet` and records
/// actual interaction data. Timestamps are UNIX timestamps recorded as 64-bit
/// integers; they do not use a Parquet timestamp time, due to out-of-range
/// values causing problems when loaded into Python.
#[derive(ArrowField)]
pub struct IntRecord {
  pub rec_id: u32,
  pub user_id: i32,
  pub book_id: i32,
  pub is_read: u8,
  pub rating: Option<f32>,
}

fn read_book_map(books: &Path) -> Result<HashMap<i32, i32>> {
  let mut map = HashMap::new();

  info!("scanning books from {:?}", books);
  let mut read = csv::Reader::from_path(books)?;
  for rec in read.deserialize() {
    let rec: RawBookMap = rec?;
    map.insert(rec.book_id_csv, rec.book_id);
  }

  Ok(map)
}

/// Scan the GoodReads simple interaction records.
pub fn scan_interaction_csv(books: &Path, records: &Path) -> Result<()> {
  let books = read_book_map(books)?;

  info!("scanning interactions from {:?}", records);
  let mut rec_id = 0;
  let pb = data_progress(0);
  let read = open_progress(records, pb.clone())?;
  let mut read = csv::Reader::from_reader(read);
  let mut write = TableWriter::open(OUT_FILE)?;
  for rec in read.deserialize() {
    let rec: RawInteraction = rec?;
    let book_id = books.get(&rec.book_id).map(|id| *id);
    let book_id = book_id.ok_or_else(|| anyhow!("unknown book ID {}", rec.book_id))?;
    rec_id += 1;
    write.write_object(IntRecord {
      rec_id,
      book_id,
      user_id: rec.user_id,
      is_read: rec.is_read as u8,
      rating: if rec.rating > 0.0 {
        Some(rec.rating)
      } else {
        None
      }
    })?;
  }

  write.finish()?;

  Ok(())
}
