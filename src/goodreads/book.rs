//! GoodReads book schemas and record processing.
use chrono::NaiveDate;
use serde::Deserialize;

use crate::arrow::*;
use crate::cleaning::isbns::*;
use crate::parsing::*;
use crate::prelude::*;

const ID_FILE: &'static str = "gr-book-ids.parquet";
const INFO_FILE: &'static str = "gr-book-info.parquet";
const SERIES_FILE: &'static str = "gr-book-series.parquet";
const AUTHOR_FILE: &'static str = "gr-book-authors.parquet";

/// The raw records we read from JSON
#[derive(Deserialize)]
pub struct RawBook {
    pub book_id: String,
    pub work_id: String,
    pub isbn: String,
    pub isbn13: String,
    pub asin: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub authors: Vec<RawAuthor>,
    #[serde(default)]
    pub publication_year: String,
    #[serde(default)]
    pub publication_month: String,
    #[serde(default)]
    pub publication_day: String,
    #[serde(default)]
    pub series: Vec<String>,
}

/// The raw author records from JSON.
#[derive(Deserialize)]
pub struct RawAuthor {
    pub author_id: String,
    #[serde(default)]
    pub role: String,
}

/// the book ID records to write to Parquet.
#[derive(ArrowField, ArrowSerialize)]
pub struct BookIdRecord {
    pub book_id: i32,
    pub work_id: Option<i32>,
    pub isbn10: Option<String>,
    pub isbn13: Option<String>,
    pub asin: Option<String>,
}

/// book info records to actually write
#[derive(ArrowField, ArrowSerialize)]
pub struct BookRecord {
    pub book_id: i32,
    pub title: Option<String>,
    pub pub_year: Option<u16>,
    pub pub_month: Option<u8>,
    pub pub_date: Option<NaiveDate>,
}

/// book series linking records
#[derive(ArrowField, ArrowSerialize)]
pub struct BookSeriesRecord {
    pub book_id: i32,
    pub series: String,
}

/// book author linking records
#[derive(ArrowField, ArrowSerialize)]
pub struct BookAuthorRecord {
    pub book_id: i32,
    pub author_id: i32,
    pub role: Option<String>,
}

/// Output handler for GoodReads books.
pub struct BookWriter {
    id_out: TableWriter<BookIdRecord>,
    info_out: TableWriter<BookRecord>,
    author_out: TableWriter<BookAuthorRecord>,
    series_out: TableWriter<BookSeriesRecord>,
}

impl BookWriter {
    pub fn open() -> Result<BookWriter> {
        let id_out = TableWriter::open(ID_FILE)?;
        let info_out = TableWriter::open(INFO_FILE)?;
        let author_out = TableWriter::open(AUTHOR_FILE)?;
        let series_out = TableWriter::open(SERIES_FILE)?;
        Ok(BookWriter {
            id_out,
            info_out,
            author_out,
            series_out,
        })
    }
}

impl DataSink for BookWriter {
    fn output_files<'a>(&'a self) -> Vec<PathBuf> {
        path_list(&[ID_FILE, INFO_FILE, AUTHOR_FILE, SERIES_FILE])
    }
}

impl ObjectWriter<RawBook> for BookWriter {
    fn write_object(&mut self, row: RawBook) -> Result<()> {
        let book_id: i32 = row.book_id.parse()?;

        self.id_out.write_object(BookIdRecord {
            book_id,
            work_id: parse_opt(&row.work_id)?,
            isbn10: trim_opt(&row.isbn)
                .map(|s| clean_asin_chars(s))
                .filter(|s| s.len() >= 7),
            isbn13: trim_opt(&row.isbn13)
                .map(|s| clean_asin_chars(s))
                .filter(|s| s.len() >= 7),
            asin: trim_opt(&row.asin)
                .map(|s| clean_asin_chars(s))
                .filter(|s| s.len() >= 7),
        })?;

        let pub_year = parse_opt(&row.publication_year)?;
        let pub_month = parse_opt(&row.publication_month)?;
        let pub_day: Option<u32> = parse_opt(&row.publication_day)?;
        let pub_date = maybe_date(pub_year, pub_month, pub_day);

        self.info_out.write_object(BookRecord {
            book_id,
            title: trim_owned(&row.title),
            pub_year,
            pub_month,
            pub_date,
        })?;

        for author in row.authors {
            self.author_out.write_object(BookAuthorRecord {
                book_id,
                author_id: author.author_id.parse()?,
                role: Some(author.role).filter(|s| !s.is_empty()),
            })?;
        }

        for series in row.series {
            self.series_out
                .write_object(BookSeriesRecord { book_id, series })?;
        }

        Ok(())
    }

    fn finish(self) -> Result<usize> {
        self.id_out.finish()?;
        self.info_out.finish()?;
        self.author_out.finish()?;
        self.series_out.finish()?;
        Ok(0)
    }
}
