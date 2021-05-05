use std::str::FromStr;
use anyhow::Result;

mod pg;
mod json;

pub use self::pg::write_pgencoded;
pub use self::json::clean_json;

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
pub fn parse_opt<T: FromStr>(s: &str) -> Result<Option<T>> where T::Err: std::error::Error + Sync + Send + 'static {
  let so = trim_opt(s);
  // we can't just use map because we need to propagate errors
  Ok(match so {
    None => None,
    Some(s) => Some(s.parse()?)
  })
}
