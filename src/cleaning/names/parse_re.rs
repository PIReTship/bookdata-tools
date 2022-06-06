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
use regex::Regex;

const YEAR_RANGE: &str = r"\d+-\d*";
const YEAR_TAG: &str = formatcp!(r"\s*,?\s*\(?({YEAR_RANGE})\)?");
const CS_NAME: &str = r"(.+?),\s*(.+?)";
const SINGLE_NAME: &str = r"[^,]*?";
const FLAG: &str = r"!!!\w+!!!";

const LINE_CS_YEAR: &str = formatcp!(r"^\s*(?:{FLAG})?{CS_NAME}{YEAR_TAG}");
const LINE_CS_JUNK: &str = formatcp!(r"^\s*(?:{FLAG})?{CS_NAME}[,.]*\s*$");
const LINE_SN_YEAR: &str = formatcp!(r"^\s*(?:{FLAG})?({SINGLE_NAME}){YEAR_TAG}");
const LINE_SN_JUNK: &str = formatcp!(r"^\s*(?:{FLAG})?({SINGLE_NAME})[,.]*\s*$");

struct ParseDefs {
  cs_year: Regex,
  cs_junk: Regex,
  sn_year: Regex,
  sn_junk: Regex,
}

lazy_static!{
  static ref DEFS: ParseDefs = ParseDefs {
    cs_year: Regex::new(LINE_CS_YEAR).unwrap(),
    cs_junk: Regex::new(LINE_CS_JUNK).unwrap(),
    sn_year: Regex::new(LINE_SN_YEAR).unwrap(),
    sn_junk: Regex::new(LINE_SN_JUNK).unwrap(),
  };
}

pub fn parse_name_entry(name: &str) -> Result<NameEntry, NameError> {
  let defs = &DEFS;
  if let Some(m) = defs.cs_year.captures(name) {
    let name = NameFmt::TwoPart(
      m.get(1).unwrap().as_str().trim().into(),
      m.get(2).unwrap().as_str().trim().into());
    Ok(NameEntry {
      name, year: m.get(3).map(|m| m.as_str().to_owned())
    })
  } else if let Some(m) = defs.cs_junk.captures(name) {
    let name = NameFmt::TwoPart(
      m.get(1).unwrap().as_str().trim().into(),
      m.get(2).unwrap().as_str().trim().into());
    Ok(NameEntry {
      name, year: None
    })
  } else if let Some(m) = defs.sn_year.captures(name) {
    let name = NameFmt::Single(m.get(1).unwrap().as_str().trim().into());
    Ok(NameEntry {
      name, year: m.get(2).map(|m| m.as_str().to_owned())
    })
  } else if let Some(m) = defs.sn_junk.captures(name) {
    let name = NameFmt::Single(m.get(1).unwrap().as_str().trim().into());
    Ok(NameEntry {
      name, year: None
    })
  } else {
    Err(NameError::Unmatched(name.to_string()))
  }
}
