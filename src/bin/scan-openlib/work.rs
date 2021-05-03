use serde::Deserialize;

use bookdata::prelude::*;
use bookdata::parquet::*;

use crate::common::Author;

#[derive(Deserialize)]
pub struct OLWorkRecord {
  authors: Vec<Author>,
}
