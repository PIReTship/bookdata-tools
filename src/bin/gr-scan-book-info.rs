use std::path::PathBuf;

use serde::Deserialize;
use chrono::NaiveDate;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::util::parsing::*;
use bookdata::cleaning::isbns::*;

/// Scan GoodReads book info into Parquet
#[derive(StructOpt)]
#[structopt(name="scan-book-info")]
pub struct ScanInteractions {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

// the records we read from JSON
#[derive(Deserialize)]
struct RawBook {
  book_id: String,
  work_id: String,
  isbn: String,
  isbn13: String,
  asin: String,
  #[serde(default)]
  title: String,
  #[serde(default)]
  publication_year: String,
  #[serde(default)]
  publication_month: String,
  #[serde(default)]
  publication_day: String,
}

// the book ID records to write to Parquet.
#[derive(TableRow)]
struct IdRecord {
  book_id: u32,
  work_id: Option<u32>,
  isbn10: Option<String>,
  isbn13: Option<String>,
  asin: Option<String>,
}

// book info records to actually write
#[derive(TableRow)]
struct InfoRecord {
  book_id: u32,
  title: Option<String>,
  pub_year: Option<u16>,
  pub_month: Option<u8>,
  pub_date: Option<NaiveDate>,
}

fn main() -> Result<()> {
  let options = ScanInteractions::from_args();
  options.common.init()?;

  info!("reading books from {:?}", &options.infile);
  let proc = LineProcessor::open_gzip(&options.infile)?;

  let mut id_out = TableWriter::open("gr-book-ids.parquet")?;
  let mut info_out = TableWriter::open("gr-book-info.parquet")?;

  for rec in proc.json_records() {
    let row: RawBook = rec?;
    let book_id: u32 = row.book_id.parse()?;

    id_out.write_object(IdRecord {
      book_id,
      work_id: parse_opt(&row.work_id)?,
      isbn10: trim_opt(&row.isbn).map(|s| clean_isbn_chars(s)),
      isbn13: trim_opt(&row.isbn13).map(|s| clean_isbn_chars(s)),
      asin: trim_opt(&row.asin).map(|s| clean_asin_chars(s))
    })?;

    let pub_year = parse_opt(&row.publication_year)?;
    let pub_month = parse_opt(&row.publication_month)?;
    let pub_day: Option<u32> = parse_opt(&row.publication_day)?;
    let pub_date = maybe_date(pub_year, pub_month, pub_day);

    info_out.write_object(InfoRecord {
      book_id,
      title: trim_owned(&row.title),
      pub_year, pub_month, pub_date
    })?;
  }

  let nlines = id_out.finish()?;
  let nl2 = info_out.finish()?;
  info!("wrote {} ID records and {} info records", nlines, nl2);

  Ok(())
}
