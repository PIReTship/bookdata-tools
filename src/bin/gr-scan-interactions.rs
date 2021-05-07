use std::path::PathBuf;

use serde::Deserialize;
use chrono::prelude::*;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::index::IdIndex;
use bookdata::parsing::*;
use bookdata::parsing::dates::*;

/// Scan GoodReads interaction file into Parquet
#[derive(StructOpt)]
#[structopt(name="scan-interactions")]
pub struct ScanInteractions {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

// the records we read from JSON
#[derive(Deserialize)]
struct RawInteraction {
  user_id: String,
  book_id: String,
  // review_id: String,
  #[serde(rename="isRead")]
  is_read: bool,
  rating: f32,
  date_added: String,
  date_updated: String,
  read_at: String,
  started_at: String,
}

// the records we're actually going to write to the table
#[derive(TableRow)]
struct IntRecord {
  rec_id: u32,
  user_id: u32,
  book_id: u32,
  is_read: u8,
  rating: Option<f32>,
  added: DateTime<FixedOffset>,
  updated: DateTime<FixedOffset>,
  read_started: Option<DateTime<FixedOffset>>,
  read_finished: Option<DateTime<FixedOffset>>,
}

fn main() -> Result<()> {
  let options = ScanInteractions::from_args();
  options.common.init()?;

  info!("reading interactions from {:?}", &options.infile);
  let proc = LineProcessor::open_gzip(&options.infile)?;
  let mut users = IdIndex::new();

  let mut writer = TableWriter::open("gr-interactions.parquet")?;
  let mut n_recs = 0;

  for rec in proc.json_records() {
    let row: RawInteraction = rec?;
    let rec_id = n_recs + 1;
    n_recs += 1;
    let key = hex::decode(row.user_id.as_bytes())?;
    let user_id = users.intern(key);
    let book_id: u32 = row.book_id.parse()?;

    writer.write_object(IntRecord {
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
  }

  let nlines = writer.finish()?;
  info!("wrote {} records for {} users", nlines, users.len());

  Ok(())
}
