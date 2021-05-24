use nom::{
  IResult, Finish,
  bytes::complete::*,
  character::complete::*,
  branch::*,
  combinator::*,
  sequence::*,
};

use thiserror::Error;
use anyhow::{Result};

use crate::parsing::combinators::*;

#[derive(Error, Debug)]
pub enum NameError {
  #[error("Error parsing name entry: {0}")]
  ParseError(nom::error::Error<String>),
}

impl From<nom::error::Error<&str>> for NameError {
  fn from(ek: nom::error::Error<&str>) -> NameError {
    NameError::ParseError(nom::error::Error::new(
      ek.input.to_owned(),
      ek.code)
    )
  }
}

enum NameFmt {
  Single(String),
  TwoPart(String, String)
}

struct NameEntry {
  name: NameFmt,
  year: Option<String>
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

/// Parse away trailing stuff
fn trailing_junk(input: &str) -> IResult<&str, ()> {
  map(tuple((one_of(",."), space0, eof)), |_| ())(input)
}

/// Parse a year range with optional trailer
fn year_range(input: &str) -> IResult<&str, String> {
  map(
    tuple((digit1, tag("-"), digit0)),
    |(y1, _s, y2)| {
      format!("{}-{}", y1, y2)
    }
  )(input)
}

/// Parse a year tag that may appear in a name, perhaps with a string
fn year_tag(input: &str) -> IResult<&str, String> {
  preceded(
    pair(opt(tuple((space0, tag(",")))), space0),
    delimited(opt(tag("(")), year_range, opt(tag(")")))
  )(input)
}

/// Last, First name construction
fn cs_name(input: &str) -> IResult<&str, NameFmt> {
  let sep = delimited(space0, tag(","), space0);
  let (rest, (last, _sep)) = take_until_parse(sep)(input)?;
  Ok(("", NameFmt::TwoPart(last.trim().to_owned(), rest.trim().to_owned())))
}

/// Undelimited name construction
fn single_name(name: &str) -> IResult<&str, NameFmt> {
  Ok(("", NameFmt::Single(name.trim().to_owned())))
}

/// Parse a full name entry
fn name_entry(entry: &str) -> IResult<&str, NameEntry> {
  alt((
    // year tag
    into(pair_nongreedy(
      alt((cs_name, single_name)),
      year_tag
    )),
    // trailing junk
    map(pair_nongreedy(
      alt((cs_name, single_name)),
      trailing_junk
    ), |(n, _)| n.into()),
    // no junk
    into(alt((cs_name, single_name)))
  ))(entry)
}

pub fn name_variants(name: &str) -> Result<Vec<String>, NameError> {
  let (_r, parse) = name_entry(name).finish()?;
  let mut variants = Vec::new();
  match parse.name {
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

  Ok(variants)
}

#[test]
fn test_year_range() {
  let text = "1712-1783";
  let (_t, res) = year_range(text).expect("parse error");
  assert_eq!(res, text);
}

#[test]
fn test_year_half_range() {
  let text = "1998-";
  let (_t, res) = year_range(text).expect("parse error");
  assert_eq!(res, text);
}

#[test]
fn test_year_tag_parens() {
  let text = "(3882-)";
  let (_t, res) = year_tag(text).expect("parse error");
  assert_eq!(res, "3882-");
}

#[test]
fn test_year_tag_no_parens() {
  let text = "3882-";
  let (_t, res) = year_tag(text).expect("parse error");
  assert_eq!(res, "3882-");
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
