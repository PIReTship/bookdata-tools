use std::str::FromStr;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use bookdata::prelude::*;
use bookdata::tsv::split_first;

/// Struct representing a row of the OpenLibrary dump file.
pub struct Row<T> {
  pub key: String,
  pub record: T
}

#[derive(Error, Debug)]
pub enum RowError {
  #[error("line has insufficient fields, failed splitting {0}")]
  FieldError(i32),
  #[error("JSON parsing error: {0}")]
  ParseError(#[from] serde_json::Error)
}

impl <T: DeserializeOwned> FromStr for Row<T> {
  type Err = RowError;

  fn from_str(s: &str) -> Result<Row<T>, RowError> {
    // split row into columns
    let (_, rest) = split_first(s).ok_or(RowError::FieldError(1))?;
    let (key, rest) = split_first(rest).ok_or(RowError::FieldError(2))?;
    let (_, rest) = split_first(rest).ok_or(RowError::FieldError(3))?;
    let (_, data) = split_first(rest).ok_or(RowError::FieldError(4))?;
    let record = serde_json::from_str(data).map_err(|e| {
      error!("invalid JSON in record {}: {:?}", key, e);
      let jsv: serde_json::Value = serde_json::from_str(data).expect("invalid JSON");
      let jsp = serde_json::to_string_pretty(&jsv).expect("uhh");
      info!("offending JSON: {}", jsp);
      e
    })?;
    Ok(Row {
      key: key.to_owned(),
      record
    })
  }
}

/// Trait for processing OL data.
pub trait OLProcessor<T> where Self: Sized {
  /// Construct a new processor.
  fn new() -> Result<Self>;

  /// Process one row
  fn process_row(&mut self, row: Row<T>) -> Result<()>;

  /// Finish writing
  fn finish(self) -> Result<()>;
}

/// Struct representing an author link in OL.
#[derive(Deserialize, Debug)]
pub struct Author {
  pub author: Option<AuLink>
}

/// Actual author link
#[derive(Deserialize, Debug)]
pub struct AuLink {
  pub key: String
}


impl Author {
  pub fn key<'a>(&'a self) -> Option<&'a str> {
    match self.author {
      Some(ref au) => Some(au.key.as_ref()),
      None => None
    }
  }
}
