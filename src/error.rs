use std::io;
use std::fmt;
use std::error::Error;

macro_attr! {
  #[derive(Debug, EnumFromInner!)]
  pub enum BDError {
    IO(io::Error),
    XML(quick_xml::Error),
    UTF8(std::str::Utf8Error),
    RDFNT(ntriple::parser::ParseError),
    PSQL(postgres::error::Error),
    Boxed(Box<Error>),
    Misc(String)
  }
}

impl fmt::Display for BDError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}", self)
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
wrap_error!(log::SetLoggerError);
wrap_error!(postgres::tls::native_tls::native_tls::Error);
wrap_error!(zip::result::ZipError);

pub fn err(msg: &str) -> BDError {
  BDError::Misc(msg.to_string())
}

/// Typedef for all-purpose result type.
pub type Result<R> = std::result::Result<R, BDError>;
