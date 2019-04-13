use derive_more::*;
use std::io;
use std::fmt;
use std::error::Error;
use crossbeam_channel;

#[derive(Debug, From)]
pub enum BDError {
  IO(io::Error),
  XML(quick_xml::Error),
  UTF8(std::str::Utf8Error),
  RDFNT(ntriple::parser::ParseError),
  PSQL(postgres::error::Error),
  Boxed(Box<Error>),
  Misc(String)
}

impl fmt::Display for BDError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

impl From<log::SetLoggerError> for BDError {
  fn from(_sle: log::SetLoggerError) -> BDError {
    err("cannot set logger")
  }
}

macro_rules! wrap_error {
  ($et:ty) => {
    impl From<$et> for BDError {
      fn from(err: $et) -> BDError {
        BDError::Boxed(Box::new(err))
      }
    }
  }
}

wrap_error!(std::env::VarError);
wrap_error!(zip::result::ZipError);
wrap_error!(crossbeam_channel::RecvError);

pub fn err(msg: &str) -> BDError {
  BDError::Misc(msg.to_string())
}

/// Typedef for all-purpose result type.
pub type Result<R> = std::result::Result<R, BDError>;
