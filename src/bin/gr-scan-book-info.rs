use std::path::PathBuf;

use serde::Deserialize;
use chrono::NaiveDate;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::parsing::*;
use bookdata::cleaning::isbns::*;

const ID_FILE: &'static str = "gr-book-ids.parquet";
const INFO_FILE: &'static str = "gr-book-info.parquet";

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
  book_id: i32,
  work_id: Option<i32>,
  isbn10: Option<String>,
  isbn13: Option<String>,
  asin: Option<String>,
}

// book info records to actually write
#[derive(TableRow)]
struct InfoRecord {
  book_id: i32,
  title: Option<String>,
  pub_year: Option<u16>,
  pub_month: Option<u8>,
  pub_date: Option<NaiveDate>,
}

struct BookWriter {
  id_out: TableWriter<IdRecord>,
  info_out: TableWriter<InfoRecord>
}

impl BookWriter {
  fn open() -> Result<BookWriter> {
    let id_out = TableWriter::open(ID_FILE)?;
    let info_out = TableWriter::open(INFO_FILE)?;
    Ok(BookWriter {
      id_out, info_out
    })
  }
}

impl ObjectWriter<RawBook> for BookWriter {
  fn write_object(&mut self, row: RawBook) -> Result<()> {
    let book_id: i32 = row.book_id.parse()?;

    self.id_out.write_object(IdRecord {
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

    self.info_out.write_object(InfoRecord {
      book_id,
      title: trim_owned(&row.title),
      pub_year, pub_month, pub_date
    })?;

    Ok(())
  }

  fn finish(self) -> Result<usize> {
    self.id_out.finish()?;
    self.info_out.finish()?;
    Ok(0)
  }
}

fn main() -> Result<()> {
  let options = ScanInteractions::from_args();
  options.common.init()?;

  info!("reading books from {:?}", &options.infile);
  let proc = LineProcessor::open_gzip(&options.infile)?;

  let mut writer = BookWriter::open()?;
  proc.process_json(&mut writer)?;
  writer.finish()?;

  info!("output {} is {}", ID_FILE, file_human_size(ID_FILE)?);
  info!("output {} is {}", INFO_FILE, file_human_size(INFO_FILE)?);

  Ok(())
}
