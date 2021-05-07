use std::str::FromStr;

use anyhow::{Result, anyhow};

use chrono::NaiveDate;

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
    Some(s) => Some(match s.parse() {
      Ok(v) => v,
      Err(e) => return Err(anyhow!("error parsing ‘{}’: {}", s, e))
    })
  })
}

/// Construct a date if all components are defined and specify a valid day
pub fn maybe_date<Y: Into<i32>, M: Into<u32>, D: Into<u32>>(year: Option<Y>, month: Option<M>, day: Option<D>) -> Option<NaiveDate> {
  match (year, month, day) {
    (Some(y), Some(m), Some(d)) => {
      let year: i32 = y.into();
      if year.abs() < 10000 {  // pandas doesn't like some years
        NaiveDate::from_ymd_opt(year, m.into(), d.into())
      } else {
        None
      }
    },
    _ => None
  }
}
