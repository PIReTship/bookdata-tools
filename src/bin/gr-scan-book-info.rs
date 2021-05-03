use std::str::FromStr;
use std::path::PathBuf;

use serde::Deserialize;

use bookdata::prelude::*;
use bookdata::parquet::*;

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
  title: String
}

// the records we're actually going to write to the table
#[derive(TableRow)]
struct IdRecord {
  book_id: u32,
  work_id: Option<u32>,
  isbn: Option<String>,
  isbn13: Option<String>,
  asin: Option<String>
}

// book info records to actually write
#[derive(TableRow)]
struct InfoRecord {
  book_id: u32,
  title: Option<String>
}

/// Trim a string, and convert to None if it is empty.
fn trim_opt<'a>(s: &'a str) -> Option<&'a str> {
  let s2 = s.trim();
  if s2.is_empty() {
    None
  } else {
    Some(s2)
  }
}

/// Trim a string, and convert to None if it is empty.
fn trim_owned(s: &str) -> Option<String> {
  return trim_opt(s).map(|s| s.to_owned())
}

/// Parse a possibly-empty string into an option.
fn parse_opt<T: FromStr>(s: &str) -> Result<Option<T>> where T::Err: std::error::Error + Sync + Send + 'static {
  let so = trim_opt(s);
  // we can't just use map because we need to propagate errors
  Ok(match so {
    None => None,
    Some(s) => Some(s.parse()?)
  })
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
      isbn: trim_owned(&row.isbn),
      isbn13: trim_owned(&row.isbn13),
      asin: trim_owned(&row.asin)
    })?;

    info_out.write_object(InfoRecord {
      book_id,
      title: trim_owned(&row.title)
    })?;
  }

  let nlines = id_out.finish()?;
  let nl2 = info_out.finish()?;
  info!("wrote {} ID records and {} info records", nlines, nl2);

  Ok(())
}
