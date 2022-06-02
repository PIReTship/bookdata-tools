use nom::{
  IResult, Finish,
  bytes::complete::*,
  character::complete::*,
  branch::*,
  combinator::*,
  sequence::*,
};

use crate::parsing::combinators::*;
use super::parse_types::*;

impl From<nom::error::Error<&str>> for NameError {
  fn from(ek: nom::error::Error<&str>) -> NameError {
    NameError::ParseError(nom::error::Error::new(
      ek.input.to_owned(),
      ek.code)
    )
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

pub fn parse_name_entry(name: &str) -> Result<NameEntry, NameError> {
  let (_r, parse) = name_entry(name).finish()?;
  Ok(parse)
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
