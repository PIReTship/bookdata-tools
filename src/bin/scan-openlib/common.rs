use std::str::FromStr;
use std::fmt;

use serde::Deserialize;
use serde::Deserializer;
use serde::de;
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

impl <T: de::DeserializeOwned> FromStr for Row<T> {
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
pub enum Author {
  Key(String),
  Object {
    author: Keyed
  },
  Empty
}

/// Keyed object reference
#[derive(Deserialize, Debug)]
pub struct Keyed {
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


// we have to manually implement Deserialize to handle truncated authors
struct AuthorVisitor;

impl <'de> Deserialize<'de> for Author {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    deserializer.deserialize_map(AuthorVisitor)
  }
}

impl <'de> de::Visitor<'de> for AuthorVisitor {
  type Value = Author;

  fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
    formatter.write_str("a string or object")
  }

  fn visit_str<E>(self, s: &str) -> Result<Self::Value, E> {
    let key = match &s[0..3] {
      "/a/" => "/author/".to_owned() + &s[3..],
      _ => s.to_owned()
    };
    Ok({Author {
      author: Some(Keyed {
        key
      })
    }})
  }

  fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
    let mut map = map;
    while let Some(e) = map.next_entry()? {
      let (k, v): (&str, Keyed) = e;
      if k == "author" {
        return Ok(Author {
          author: Some(v)
        })
      }
    }

    // no author found
    Ok(Author {
      author: None
    })
  }
}
