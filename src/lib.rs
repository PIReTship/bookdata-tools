#[macro_use]
extern crate log;

extern crate quick_xml;
extern crate postgres;
extern crate streaming_iterator;

pub mod cleaning;
pub mod tsv;
pub mod db;

use std::io;
use std::fmt;
use std::error::Error;

#[derive(Debug)]
pub enum BDError {
  IO(io::Error),
  XML(quick_xml::Error),
  UTF8(std::str::Utf8Error),
  PSQL(postgres::error::Error),
  Misc(String)
}

impl fmt::Display for BDError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

impl From<io::Error> for BDError {
  fn from(err: io::Error) -> BDError {
    BDError::IO(err)
  }
}

impl From<std::str::Utf8Error> for BDError {
  fn from(err: std::str::Utf8Error) -> BDError {
    BDError::UTF8(err)
  }
}

impl From<quick_xml::Error> for BDError {
  fn from(err: quick_xml::Error) -> BDError {
    BDError::XML(err)
  }
}

impl From<postgres::error::Error> for BDError {
  fn from(err: postgres::error::Error) -> BDError {
    BDError::PSQL(err)
  }
}

pub fn err(msg: &str) -> BDError {
  BDError::Misc(msg.to_string())
}

/// Typedef for all-purpose result type.
pub type Result<R> = std::result::Result<R, BDError>;

/// Initialize logging
pub fn log_init(quiet: bool, level: usize) -> std::result::Result<(), Box<std::error::Error>> {
  Ok(stderrlog::new().verbosity(level + 2).quiet(quiet).init()?)
}
