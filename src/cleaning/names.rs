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
use std::borrow::Cow;

use anyhow::{Result};
use regex::Regex;
use lazy_static::lazy_static;

use super::strings::norm_unicode;

mod parse_types;
#[cfg(feature="nom")]
mod parse_nom;
#[cfg(feature="peg")]
mod parse_peg;
mod parse_re;

pub use parse_types::NameError;
use parse_types::NameFmt;

#[cfg(feature="reparse")]
pub use parse_re::parse_name_entry;
#[cfg(feature="nom")]
pub use parse_nom::parse_name_entry;
#[cfg(feature="peg")]
pub use parse_peg::parse_name_entry;

lazy_static! {
  static ref NAME_CLEAN_RE: Regex = Regex::new(r"[\s.]+").unwrap();
}

/// Clean up a name from unnecessary special characters.
pub fn clean_name<'a>(name: &'a str) -> String {
  let name = norm_unicode(name);
  let name = NAME_CLEAN_RE.replace_all(&name, " ");
  name.to_string()
}

/// Extract all variants from a name.
///
/// See the [module documentation][self] for details on this parsing process.
pub fn name_variants(name: &str) -> Result<Vec<String>, NameError> {
  let parse = parse_name_entry(name)?;
  let mut variants = Vec::new();
  match parse.name.simplify() {
    NameFmt::Empty => (),
    NameFmt::Single(n) => variants.push(n),
    NameFmt::TwoPart(last, first) => {
      variants.push(format!("{} {}", first, last));
      variants.push(format!("{}, {}", last, first));
    }
  };

  // create a version with the year
  if let Some(y) = parse.year {
    for i in 0..variants.len() {
      variants.push(format!("{}, {}", variants[i], y));
    }
  }


  let variants = variants.iter().map(|s| clean_name(s)).collect();

  Ok(variants)
}

#[cfg(test)]
fn check_name_decode(name: &str, exp_variants: &[&str]) {
  let dec_variants = name_variants(name).expect("parse error");
  println!("scanned name {}:", name);
  for v in &dec_variants {
    println!("- {}", v);
  }
  assert_eq!(dec_variants.len(), exp_variants.len());
  for n in exp_variants {
    assert!(dec_variants.contains(&(*n).to_owned()), "expected variant {} not found", n);
  }
}

#[test]
fn test_first_last() {
  check_name_decode("Mary Sumner", &["Mary Sumner"]);
}

#[test]
fn test_trim() {
  check_name_decode("Mary Sumner.", &["Mary Sumner"]);
}

#[test]
fn test_last_first_variants() {
  check_name_decode("Sequeira Moreno, Francisco", &[
    "Sequeira Moreno, Francisco",
    "Francisco Sequeira Moreno"
  ]);
}

#[test]
fn test_last_first_punctuation() {
  check_name_decode("Jomaa-Raad, Wafa,", &[
    "Wafa Jomaa-Raad",
    "Jomaa-Raad, Wafa",
  ]);
}

#[test]
fn test_last_first_year() {
  check_name_decode("Morgan, Michelle, 1967-", &[
    "Morgan, Michelle, 1967-",
    "Morgan, Michelle",
    "Michelle Morgan",
    "Michelle Morgan, 1967-"
  ]);
}

#[test]
fn test_first_last_year() {
  check_name_decode("Ditlev Reventlow (1712-1783)", &[
    "Ditlev Reventlow, 1712-1783",
    "Ditlev Reventlow",
  ]);
}

#[test]
fn test_trailing_comma() {
  check_name_decode("Miller, Pat Zietlow,", &[
    "Pat Zietlow Miller",
    "Miller, Pat Zietlow",
  ]);
}

#[test]
fn test_trailing_punctuation() {
  check_name_decode("Miller, Pat Zietlow,.", &[
    "Pat Zietlow Miller",
    "Miller, Pat Zietlow",
  ]);
}

#[test]
fn test_single_trailing() {
  let parse = parse_name_entry("Manopoly,").expect("parse error");
  assert!(parse.year.is_none());
  assert_eq!(parse.name, NameFmt::Single("Manopoly".to_string()));
  check_name_decode("Manopoly,", &["Manopoly"]);
}

#[test]
fn test_locked() {
  check_name_decode("!!!GESPERRT!!!Moro, Simone", &[
    "Simone Moro",
    "Moro, Simone"
  ]);
}

#[test]
fn test_single_initial() {
  check_name_decode("Navarro, P.", &[
    "Navarro, P",
    "P Navarro",
  ])
}

#[test]
fn test_leading_comma() {
  check_name_decode(", Engelbert", &[
    "Engelbert"
  ]);
}

#[test]
fn test_year_only() {
  check_name_decode("1941-", &[]);
}
