//! Extract and normalize author names.
//!
//! Names in the book data — both in author records and in their references in
//! book records — come in a variety of formats.  This module is responsible for
//! expanding and normalizing those name formats to improve data linkability.
//! Some records also include a year or date range for the author's lifetime. We
//! normalize names as follows:
//!
//! - If the name is “Last, First”, we emit both “Last, First” and “First Last”
//!   variants.
//! - If the name has a year, we emit each variant both with and without the
//!   year.
//! - Leading and trailing junk is cleaned
//!
//! This maximizes our ability to match records across sources recording names
//! in different formats.
//!
//! [`name_variants`] is the primary entry point for using this module.

use super::parse_types::*;

use const_format::*;
use lazy_static::lazy_static;
use regex::*;

const YEAR_RANGE: &str = r"\d+-\d*";
const YEAR_TAG: &str = formatcp!(r"\s*,?\s*\(?(?P<year>{YEAR_RANGE})\)?");
const CS_NAME: &str = r"(?P<last>.+?),\s*(?P<rest>.+?)";
const SINGLE_NAME: &str = r",?\s*(?P<single>[^,]*?)";
const FLAG: &str = r"!!!\w+!!!";

// const LINE_CS_YEAR: &str = formatcp!(r"^\s*(?:{FLAG})?{CS_NAME}{YEAR_TAG}");
// const LINE_CS_JUNK: &str = formatcp!(r"^\s*(?:{FLAG})?{CS_NAME}[,.]*\s*$");
// const LINE_SN_YEAR: &str = formatcp!(r"^\s*(?:{FLAG})?,?\s*({SINGLE_NAME}){YEAR_TAG}");
// const LINE_SN_JUNK: &str = formatcp!(r"^\s*(?:{FLAG})?,?\s*({SINGLE_NAME})[,.]*\s*$");

const NAME_RE_STR: &str = formatcp!(r"^\s*(?:{FLAG})?(?:{CS_NAME}|{SINGLE_NAME})(?:{YEAR_TAG}|[,.]*\s*$)");

lazy_static!{
  static ref NAME_RE: Regex = Regex::new(NAME_RE_STR).unwrap();
}

fn trim_match<'a>(mm: Option<Match<'a>>) -> Option<&'a str> {
  mm.map(|m| m.as_str().trim()).filter(|s| !s.is_empty())
}

pub fn parse_name_entry(name: &str) -> Result<NameEntry, NameError> {
  let name_re = &NAME_RE;

  // debug testing
  #[cfg(test)]
  println!("regex: {}", NAME_RE_STR);
  #[cfg(test)]
  println!("matching “{}”", name);

  if let Some(caps) = name_re.captures(name) {
    let last = trim_match(caps.name("last"));
    let rest = trim_match(caps.name("rest"));
    let name = match (last, rest) {
      (Some(l), Some(r)) => NameFmt::TwoPart(l.to_string(), r.to_string()),
      (None, Some(r)) => NameFmt::Single(r.to_string()),
      (Some(l), None) => NameFmt::Single(l.to_string()),
      (None, None) => match trim_match(caps.name("single")) {
        Some(s) => NameFmt::Single(s.to_string()),
        None => NameFmt::Empty
      }
    };

    let year = trim_match(caps.name("year")).map(|s| s.to_string());

    Ok(NameEntry {
      name, year
    })
  } else {
    Err(NameError::Unmatched(name.to_string()))
  }
}
