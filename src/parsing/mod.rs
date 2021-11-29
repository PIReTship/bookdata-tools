//! Parsing utilities.  Format-specific parsing is in various other modules.
use std::str::FromStr;
use std::error::{Error as StdError};

use anyhow::{Result, anyhow};

pub mod combinators;
pub mod dates;
#[cfg(test)]
mod test_dates;

/// Trim a string, and convert to None if it is empty.
pub fn trim_opt<'a>(s: &'a str) -> Option<&'a str> {
  let s2 = s.trim();
  if s2.is_empty() {
    None
  } else {
    Some(s2)
  }
}

/// Trim a string, and convert to None if it is empty.
pub fn trim_owned(s: &str) -> Option<String> {
  return trim_opt(s).map(|s| s.to_owned())
}

/// Parse a possibly-empty string into an option.
pub fn parse_opt<T: FromStr>(s: &str) -> Result<Option<T>> where T::Err: StdError + Sync + Send + 'static {
  let so = trim_opt(s);
  // we can't just use map because we need to propagate errors
  Ok(match so {
    None => None,
    Some(s) => Some(match s.parse() {
      Ok(v) => v,
      Err(e) => return Err(anyhow!("error parsing ‘{}’: {}", s, e))
    })
  })
}

pub use dates::maybe_date;
