use serde::Deserialize;
use chrono::NaiveDate;

use crate::prelude::*;
use crate::arrow::*;
use crate::parsing::*;

const OUT_FILE: &'static str = "gr-work-info.parquet";

// Work records from JSON.
#[derive(Deserialize)]
pub struct RawWork {
  pub work_id: String,
  #[serde(default)]
  pub original_title: String,
  #[serde(default)]
  pub original_publication_year: String,
  #[serde(default)]
  pub original_publication_month: String,
  #[serde(default)]
  pub original_publication_day: String,
}

// Rows in the processed work Parquet table.
#[derive(ParquetRecordWriter)]
pub struct WorkRecord {
  pub work_id: i32,
  pub title: Option<String>,
  pub pub_year: Option<i16>,
  pub pub_month: Option<u8>,
  pub pub_date: Option<NaiveDate>
}

// Object writer to transform and write GoodReads works
pub struct WorkWriter {
  writer: TableWriter<WorkRecord>,
  n_recs: usize,
}

impl WorkWriter {
  // Open a new output
  pub fn open() -> Result<WorkWriter> {
    let writer = TableWriter::open(OUT_FILE)?;
    Ok(WorkWriter {
      writer,
      n_recs: 0
    })
  }
}

impl DataSink for WorkWriter {
  fn output_files(&self) -> Vec<PathBuf> {
    path_list(&[OUT_FILE])
  }
}

impl ObjectWriter<RawWork> for WorkWriter {
  fn write_object(&mut self, row: RawWork) -> Result<()> {
    let work_id: i32 = row.work_id.parse()?;

    let pub_year = parse_opt(&row.original_publication_year)?;
    let pub_month = parse_opt(&row.original_publication_month)?;
    let pub_day: Option<u32> = parse_opt(&row.original_publication_day)?;
    let pub_date = maybe_date(pub_year, pub_month, pub_day);

    self.writer.write_object(WorkRecord {
      work_id,
      title: trim_owned(&row.original_title),
      pub_year, pub_month, pub_date
    })?;
    self.n_recs += 1;
    Ok(())
  }

  fn finish(self) -> Result<usize> {
    self.writer.finish()?;
    Ok(self.n_recs)
  }
}
