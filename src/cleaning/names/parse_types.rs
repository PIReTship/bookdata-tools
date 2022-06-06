use thiserror::Error;

#[derive(Error, Debug)]
pub enum NameError {
  #[cfg(feature="nom")]
  #[error("Error parsing name entry: {0}")]
  ParseError(nom::error::Error<String>),
  #[cfg(feature="peg")]
  #[error("Error parsing name entry: {0}")]
  PEGError(#[from] peg::error::ParseError<peg::str::LineCol>),
  #[error("Could not match {0}")]
  Unmatched(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum NameFmt {
  Single(String),
  TwoPart(String, String),
  Empty
}

impl NameFmt {
  pub fn simplify(self) -> NameFmt {
    match self {
      NameFmt::TwoPart(l, f) if f.is_empty() => {
        NameFmt::Single(l)
      },
      NameFmt::Single(n) if n.is_empty() => NameFmt::Empty,
      _ => self
    }
  }
}

#[derive(Debug, PartialEq, Eq)]
pub struct NameEntry {
  pub name: NameFmt,
  pub year: Option<String>
}

impl From<(NameFmt, String)> for NameEntry {
  fn from(t: (NameFmt, String)) -> NameEntry {
    let (name, year) = t;
    NameEntry {
      name, year: Some(year)
    }
  }
}

impl From<NameFmt> for NameEntry {
  fn from(name: NameFmt) -> NameEntry {
    NameEntry {
      name, year: None
    }
  }
}
