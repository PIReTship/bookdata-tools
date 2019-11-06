use std::io::prelude::*;

use crate::cleaning::{write_pgencoded, clean_json};
use crate::error::Result;

/// Trait defining target tables and parsing behavior for a data set
pub trait DataSetOps {
  /// Get the default schema for this data set
  fn schema(&self) -> &'static str;

  /// Get the SQL table name for the requested table and this data set
  fn table_name(&self, tbl: &str) -> String;

  /// Get the column names for importing this data set
  fn columns(&self, tbl: &str) -> Vec<String>;

  /// Import data for this data set
  ///
  /// This method process the input source data `src` and writes it out to the
  /// PostgreSQL copy stream `dst`.  It returns the number of records imported.
  fn import(&self, src: &mut dyn BufRead, dst: &mut dyn Write) -> Result<usize>;
}

/// Import raw JSON records into a single column.
///
/// This function reads in each line of JSON, cleans it up, and writes it out to the
/// PostgreSQL copy stream (properly encoded) for a single-column import.
pub fn process_raw<R: BufRead, W: Write>(src: &mut R, dst: &mut W) -> Result<usize> {
  let mut jsbuf = String::new();
  let mut n = 0;
  for line in src.lines() {
    let json = line?;
    clean_json(&json, &mut jsbuf);
    write_pgencoded(dst, jsbuf.as_bytes())?;
    dst.write_all(b"\n")?;
    n += 1;
  }

  Ok(n)
}
