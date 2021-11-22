//! OpenLibrary author schemas.
use serde::{Deserialize};

use crate::cleaning::strings::norm_unicode;
use crate::prelude::*;
use crate::arrow::*;

/// An author record parsed from OpenLibrary JSON.
#[derive(Deserialize)]
pub struct OLAuthorSource {
  #[serde(default)]
  pub name: Option<String>,
  #[serde(default)]
  pub personal_name: Option<String>,
  #[serde(default)]
  pub alternate_names: Vec<String>
}

impl OLAuthorSource {
  /// Get a list of author name records for this author.
  pub fn names(&self, id: u32) -> Vec<AuthorNameRec> {
    let mut names = Vec::new();

    if let Some(n) = &self.name {
      names.push(AuthorNameRec {
        id, source: b'n', name: norm_unicode(&n).into_owned()
      });
    }

    if let Some(n) = &self.personal_name {
      names.push(AuthorNameRec {
        id, source: b'p', name: norm_unicode(&n).into_owned()
      });
    }

    for n in &self.alternate_names {
      names.push(AuthorNameRec {
        id, source: b'a', name: norm_unicode(&n).into_owned()
      });
    }

    names
  }
}

/// An author record in the extracted Parquet.
#[derive(TableRow)]
pub struct AuthorRec {
  pub id: u32,
  pub key: String,
  pub name: Option<String>
}

/// An author-name record in the extracted Parquet.
#[derive(TableRow)]
pub struct AuthorNameRec {
  pub id: u32,
  pub source: u8,
  pub name: String
}
