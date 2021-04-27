use std::str::FromStr;
use std::path::PathBuf;

use serde::{Deserialize};
use serde_json::from_str;

use bookdata::prelude::*;
use bookdata::parquet::*;
use bookdata::index::IdIndex;

/// Scan GoodReads interaction file into Parquet
#[derive(StructOpt)]
#[structopt(name="scan-interactions")]
pub struct ScanInteractions {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf,

  /// Ouptut file
  #[structopt(name = "OUTPUT", parse(from_os_str))]
  outfile: PathBuf
}

// the records we read from JSON
#[derive(Deserialize)]
struct RawInteraction {
  user_id: String,
  book_id: String,
  // review_id: String,
  #[serde(rename="isRead")]
  is_read: bool,
  rating: i32,
  // date_added: String
}

impl FromStr for RawInteraction {
  type Err = serde_json::Error;

  fn from_str(s: &str) -> serde_json::Result<RawInteraction> {
    return from_str(s)
  }
}

// the records we're actually going to write to the table
#[derive(TableRow)]
struct IntRecord {
  rec_id: u32,
  user_id: u32,
  book_id: u64,
  is_read: u8,
  rating: Option<i8>
}

fn main() -> Result<()> {
  let options = ScanInteractions::from_args();
  options.common.init()?;

  info!("reading interactions from {:?}", &options.infile);
  let proc = LineProcessor::open_gzip(&options.infile)?;
  let mut users = IdIndex::new();

  info!("writing interactions to {:?}", &options.outfile);
  let mut writer = TableWriter::open(&options.outfile)?;
  let mut n_recs = 0;

  for rec in proc.records() {
    let row: RawInteraction = rec?;
    let rec_id = n_recs + 1;
    n_recs += 1;
    let key = hex::decode(row.user_id.as_bytes())?;
    let user_id = users.intern(key);
    let book_id: u64 = row.book_id.parse()?;
    let record = IntRecord {
      rec_id, user_id, book_id,
      is_read: row.is_read as u8,
      rating: if row.rating > 0 {
        Some(row.rating as i8)
      } else {
        None
      }
    };
    writer.write(&record)?;
  }

  let nlines = writer.finish()?;
  info!("wrote {} records for {} users", nlines, users.len());

  Ok(())
}
