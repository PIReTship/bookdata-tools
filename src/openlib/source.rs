//! Parse OpenLibrary JSON.
use std::str::FromStr;

use log::*;
use serde::de;
use serde::Deserialize;
use thiserror::Error;

use crate::tsv::split_first;

/// Struct representing a row of the OpenLibrary dump file.
///
/// The row extracts the key and record, deserializing the record from JSON to the
/// appropriate type.
pub struct Row<T> {
    pub key: String,
    pub record: T,
}

/// Error type for parsing an OpenLibrary JSON row.
#[derive(Error, Debug)]
pub enum RowError {
    #[error("line has insufficient fields, failed splitting {0}")]
    FieldError(i32),
    #[error("JSON parsing error: {0}")]
    ParseError(#[from] serde_json::Error),
}

impl<T: de::DeserializeOwned> FromStr for Row<T> {
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
            record,
        })
    }
}

/// Struct representing an author link in OL.
///
/// There are several different formats in which we can find author references.
/// This enum encapsulates them, and Serde automatically deserializes it into the
/// appropraite variant.  We then use the [Author::key] function to extract the key itself,
/// no matter the variant.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Author {
    Object { key: String },
    Nested { author: Keyed },
    Key(String),
    Empty {},
}

/// Keyed object reference
#[derive(Deserialize, Debug)]
pub struct Keyed {
    pub key: String,
}

impl Author {
    /// Get the key out of an author reference.
    pub fn key<'a>(&'a self) -> Option<&'a str> {
        match self {
            Author::Object { key } => Some(key.as_ref()),
            Author::Nested { author } => Some(author.key.as_ref()),
            Author::Key(ref ks) => Some(ks.as_ref()),
            Author::Empty {} => None,
        }
    }
}

/// An author record parsed from OpenLibrary JSON.
#[derive(Deserialize)]
pub struct OLAuthorSource {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub personal_name: Option<String>,
    #[serde(default)]
    pub alternate_names: Vec<String>,
}

/// An edition record parsed from OpenLibrary JSON.
#[derive(Deserialize)]
pub struct OLEditionRecord {
    #[serde(default)]
    pub isbn_10: Vec<String>,
    #[serde(default)]
    pub isbn_13: Vec<String>,
    #[serde(default)]
    pub asin: Vec<String>,

    #[serde(default)]
    pub title: Option<String>,

    #[serde(default)]
    pub works: Vec<Keyed>,
    #[serde(default)]
    pub authors: Vec<Author>,

    #[serde(flatten)]
    pub subjects: OLSubjects,
}

/// An author record parsed from OpenLibrary JSON.
#[derive(Deserialize)]
pub struct OLWorkRecord {
    #[serde(default)]
    pub authors: Vec<Author>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(flatten)]
    pub subjects: OLSubjects,
}

/// Text entries
#[derive(Deserialize, Clone)]
#[serde(untagged)]
pub enum Text {
    String(String),
    Object { value: String },
}

impl Into<String> for Text {
    fn into(self) -> String {
        match self {
            Text::String(s) => s,
            Text::Object { value } => value,
        }
    }
}

/// Information about a work or edition's subjects.
#[derive(Deserialize)]
pub struct OLSubjects {
    #[serde(default)]
    pub subjects: Vec<Text>,
    #[serde(default)]
    pub subject_people: Vec<Text>,
    #[serde(default)]
    pub subject_places: Vec<Text>,
    #[serde(default)]
    pub subject_times: Vec<Text>,
}
