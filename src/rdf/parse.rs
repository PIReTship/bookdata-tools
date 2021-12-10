//! Parse RDF N-triples.
//!
//! This module contains a parser for the [RDF N-triple format][nt], so we don't
//! have to pull in all of Oxigraph, can be more robust to bad data formats, and
//! improve parsing performnce.  Why do we need PEG to parse n-triples?
//!
//! [nt]: https://www.w3.org/TR/n-triples/
use std::str::FromStr;

use thiserror::Error;

use nom::{
  Parser, IResult, Finish,
  bytes::complete::{take, take_while, take_while1, tag, escaped_transform},
  character::complete::{satisfy, one_of, space0, space1, alpha1, alphanumeric1},
  multi::many0,
  branch::alt,
  sequence::*,
  combinator::*,
};

use super::model::*;

#[derive(Error, Debug)]
pub enum ParseError {
  #[error("Error parsing line: {0}")]
  ParseError(nom::error::Error<String>),
}

impl From<nom::error::Error<&str>> for ParseError {
  fn from(ek: nom::error::Error<&str>) -> ParseError {
    ParseError::ParseError(nom::error::Error::new(
      ek.input.to_owned(),
      ek.code)
    )
  }
}

fn is_blank_lead(c: char) -> bool {
  let seqs = [
    ('a', 'z'),
    ('A', 'Z'),
    ('\u{00C0}', '\u{00D6}'),
    ('\u{00D8}', '\u{00F6}'),
    ('\u{00F8}', '\u{02FF}'),
    ('\u{0370}', '\u{037D}'),
    ('\u{037F}', '\u{1FFF}'),
    ('\u{200C}', '\u{200D}'),
    ('\u{2070}', '\u{218F}'),
    ('\u{2C00}', '\u{2FEF}'),
    ('\u{3001}', '\u{D7FF}'),
    ('\u{F900}', '\u{FDCF}'),
    ('\u{FDF0}', '\u{FFFD}'),
    ('\u{10000}', '\u{EFFFF}'),
    ('0', '9'),
  ];
  for (lb, ub) in &seqs {
    if c >= *lb && c <= *ub {
      return true;
    }
  }
  if c == '_' || c == ':' {
    return true;
  }
  false
}

fn is_blank_mid(c: char) -> bool {
  if is_blank_lead(c) {
    true
  } else if c == '\u{00B7}' || c == '.' || c == '-' {
    true
  } else if c >= '\u{0300}' && c <= '\u{036F}' {
    true
  } else if c >= '\u{203F}' && c <= '\u{2040}' {
    true
  } else {
    false
  }
}

fn is_irichar(c: char) -> bool {
  match c {
    c if (c as u32) < 20 => false,
    '<' => false,
    '>' => false,
    '"' => false,
    '{' => false,
    '}' => false,
    '|' => false,
    '^' => false,
    '`' => false,
    '\\' => false,
    _ => true
  }
}

fn is_strchr(c: char) -> bool {
  match c as u32 {
    0x22 => false,
    0x5c => false,
    0x0a => false,
    0x0d => false,
    _ => true
  }
}

fn decode_uchars(chars: &str) -> char {
  let cp: u32 = u32::from_str_radix(chars, 16).expect("invalid uchars");
  char::from_u32(cp).unwrap()  // better error handling for escapes
}

fn decode_esc(c: char) -> char {
  match c {
    't' => '\t',
    'b' => '\x07',
    'n' => '\n',
    'r' => '\r',
    'f' => '\x12',
    '\'' => '\'',
    '"' => '"',
    '\\' => '\\',
    _ => unreachable!("invalid escape character")
  }
}

fn uescape(input: &str) -> IResult<&str, char> {
  let u4 = preceded(tag("u"), take(4usize).map(decode_uchars));
  let u8 = preceded(tag("U"), take(8usize).map(decode_uchars));
  alt((u4, u8))(input)
}

fn escape(input: &str) -> IResult<&str, char> {
  let u4 = preceded(tag("u"), take(4usize).map(decode_uchars));
  let u8 = preceded(tag("U"), take(8usize).map(decode_uchars));
  let cs = one_of(r#"tbnrf"'\"#).map(decode_esc);
  alt((cs, u4, u8))(input)
}

fn iri_lit(input: &str) -> IResult<&str, String> {
  escaped_transform(
    take_while1(is_irichar),
    '\\',
    uescape
  )(input)
}

fn iri_ref(input: &str) -> IResult<&str, String> {
  map(
    delimited(tag("<"), opt(iri_lit), tag(">")),
    |o| o.unwrap_or_default()
  )(input)
}

// // parse a blank label. we can be more efficient like this.
// fn blank_label(input: &str) -> IResult<&str, String> {
//   if input.is_empty() {
//     return Err(NErr::Error((input, ErrorKind::Satisfy)));
//   }

//   if !is_blank_lead(input[0]) {
//     return Err(NErr::Error((input, ErrorKind::Satisfy)));
//   }


// }

fn blank_ref(input: &str) -> IResult<&str, String> {
  let first = satisfy(is_blank_lead);
  let rest = verify(
    take_while(is_blank_mid),
    |s: &str| s.as_bytes()[s.len() - 1] != b'.'  // '.' is not a valid close for a node
  );
  let label = pair(first, opt(rest));
  let label = map(label, |parts: (char, Option<&str>)| {
    let (f, r) = parts;
    if let Some(rest) = r {
      let mut res = String::with_capacity(rest.len() + 1);
      res.push(f);
      res.push_str(rest);
      res
    } else {
      String::from(f)
    }
  });
  preceded(tag("_:"), label)(input)
}


fn lang_tag(input: &str) -> IResult<&str, String> {
  let lt1 = alpha1;
  let ltres = many0(
    preceded(tag("-"), alphanumeric1)
  );
  map(preceded(tag("@"), pair(lt1, ltres)), |args| {
    let (tag, mut v) = args;
    v.insert(0, tag);
    v.join("-")
  })(input)
}

fn string_lit(input: &str) -> IResult<&str, String> {
  map(
    delimited(
      tag("\""),
      opt(escaped_transform(
        take_while1(is_strchr),
        '\\',
        escape
      )),
      tag("\"")
    ),
    |o| o.unwrap_or_default()
  )(input)
}

fn literal(input: &str) -> IResult<&str, Literal> {
  let schema = preceded(tag("^^"), iri_ref);
  map(
    tuple((string_lit, opt(lang_tag), opt(schema))),
    |(l, t, s)| Literal {
      value: l,
      lang: t,
      schema: s
    }
  )(input)
}

fn subject(input: &str) -> IResult<&str, Node> {
  let named = map(iri_ref, Node::named);
  let blank = map(blank_ref, Node::blank);
  alt((named, blank))(input)
}

fn object(input: &str) -> IResult<&str, Term> {
  let named = map(iri_ref, Term::named);
  let blank = map(blank_ref, Term::blank);
  let lit = map(literal, Term::Literal);
  alt((named, blank, lit))(input)
}

fn triple(input: &str) -> IResult<&str, Triple> {
  let seq = tuple((
    subject,
    space1,
    iri_ref,
    space1,
    object,
    space0,
    tag(".")
  ));
  let mut res = map(seq, |(s,_,p,_,o,_,_)| {
    Triple {
      subject: s,
      predicate: p,
      object: o
    }
  });
  res(input)
}

fn nt_line(input: &str) -> IResult<&str, Option<Triple>> {
  let parse = tuple((
    // whitespace is fine
    space0,
    // we might not even have a triple
    opt(triple),
    // more space is fine
    space0,
    // and a comment
    opt(preceded(tag("#"), rest)),
    eof
  ));
  map(parse, |t| t.1)(input)
}

/// Parse a triple
pub fn parse_triple(line: &str) -> Result<Option<Triple>, ParseError> {
  // try to parse a triple
  let (_, trip) = nt_line(line).finish()?;
  Ok(trip)
}

impl FromStr for Triple {
  type Err = ParseError;

  fn from_str(line: &str) -> Result<Triple, ParseError> {
    match parse_triple(line) {
      Ok(Some(t)) => Ok(t),
      Ok(None) => Ok(Triple::empty()),
      Err(e) => Err(e)
    }
  }
}

/// Decode an IRI and replace escape characters.
#[cfg(test)]
fn decode_iri(iri: &str) -> String {
  let (_, res) = iri_ref(iri).finish().expect("parse error");
  res.into()
}

/// Decode a string literal and replace escape characters
#[cfg(test)]
fn decode_lit(lit: &str) -> String {
  let (_, res) = string_lit(lit).finish().expect("parse error");
  res.into()
}

/// Decode a node
#[cfg(test)]
fn decode_node(subj: &str) -> Node {
  let (_, node) = subject(subj).finish().expect("parse error");
  node
}

#[test]
fn test_iri_decode_empty() {
  let res = decode_iri("<>");
  assert_eq!(res.as_str(), "");
}

#[test]
fn test_iri_decode_url() {
  let res = decode_iri("<https://example.com>");
  assert_eq!(res.as_str(), "https://example.com");
}

#[test]
fn test_iri_decode_escape() {
  let res = decode_iri("<https://example.com/\\u2665>");
  assert_eq!(res.as_str(), "https://example.com/♥");
}

#[test]
fn test_iri_decode_escapes() {
  let res = decode_iri("<https://example.com/\\u2665/\\U0001F49E>");
  assert_eq!(res.as_str(), "https://example.com/♥/💞");
}

#[test]
fn test_lit_decode_empty() {
  let res = decode_lit("\"\"");
  assert_eq!(res.as_str(), "");
}

#[test]
fn test_lit_decode_url() {
  let res = decode_lit("\"hello\"");
  assert_eq!(res.as_str(), "hello");
}

#[test]
fn test_lit_decode_escape() {
  let res = decode_lit("\"HACKEM \\u2665 MUCHE\"");
  assert_eq!(res.as_str(), "HACKEM ♥ MUCHE");
}

#[test]
fn test_lit_decode_escapes() {
  let res = decode_lit("\"FOOBIE \\u2665 BLETCH \\U0001F49E\"");
  assert_eq!(res.as_str(), "FOOBIE ♥ BLETCH 💞");
}

#[test]
fn test_lit_decode_cescape() {
  let res = decode_lit("\"FOOBIE \\r BLETCH\"");
  assert_eq!(res.as_str(), "FOOBIE \r BLETCH");
}


#[test]
fn test_decode_iri_node() {
  let node = decode_node("<https://example.com/wumpus>");
  assert_eq!(node, Node::named("https://example.com/wumpus"))
}

#[test]
fn test_decode_blank_node() {
  let node = decode_node("_:BBQ");
  assert_eq!(node, Node::blank("BBQ"))
}

#[test]
#[should_panic]
fn test_decode_invalid_node() {
  decode_node("wombat");
}

#[test]
fn test_parse_blank_line() {
  let res = parse_triple("");
  let res = res.expect("parse error");
  assert_eq!(res, None);
}

#[test]
fn test_parse_whitespace_line() {
  let res = parse_triple("   ");
  let res = res.expect("parse error");
  assert_eq!(res, None);
}

#[test]
fn test_parse_comment_line() {
  let res = parse_triple("# this is a comment line");
  let res = res.expect("parse error");
  assert_eq!(res, None);
}

#[test]
fn test_parse_triple_line() {
  let res = parse_triple("<hackem> <muche> <self> .");
  let res = res.expect("parse failed");
  assert!(res.is_some());
  let res = res.unwrap();
  assert_eq!(res.subject, Node::named("hackem"));
  assert_eq!(res.predicate.as_str(), "muche");
  assert_eq!(res.object, Term::named("self"));
}

#[test]
fn test_parse_triple_blank_subj() {
  let res = parse_triple("_:unmarked <muche> <self> .");
  let res = res.expect("parse failed");
  assert!(res.is_some());
  let res = res.unwrap();
  assert_eq!(res.subject, Node::blank("unmarked"));
  assert_eq!(res.predicate.as_str(), "muche");
  assert_eq!(res.object, Term::named("self"));
}

#[test]
fn test_parse_triple_blank_obj() {
  let res = parse_triple("<hackem> <muche> _:blank .");
  let res = res.expect("parse failed");
  assert!(res.is_some());
  let res = res.unwrap();
  assert_eq!(res.subject, Node::named("hackem"));
  assert_eq!(res.predicate.as_str(), "muche");
  assert_eq!(res.object, Term::blank("blank"));
}

#[test]
fn test_parse_triple_literal() {
  let res = parse_triple(r#"<hackem> <muche> "wumpus" ."#);
  let res = res.expect("parse failed");
  assert!(res.is_some());
  let res = res.unwrap();
  assert_eq!(res.subject, Node::named("hackem"));
  assert_eq!(res.predicate.as_str(), "muche");
  assert_eq!(res.object, Term::literal("wumpus"));
}

#[test]
fn test_parse_triple_literal_lang() {
  let res = parse_triple(r#"<hackem> <muche> "wumpus"@en-US ."#);
  let res = res.expect("parse failed");
  assert!(res.is_some());
  let res = res.unwrap();
  assert_eq!(res.subject, Node::named("hackem"));
  assert_eq!(res.predicate.as_str(), "muche");
  assert_eq!(res.object, Term::literal_lang("wumpus", "en-US"));
}

#[test]
fn test_parse_triple_literal_schema() {
  let res = parse_triple(r#"<hackem> <muche> "wumpus"^^<schema> ."#);
  let res = res.expect("parse failed");
  assert!(res.is_some());
  let res = res.unwrap();
  assert_eq!(res.subject, Node::named("hackem"));
  assert_eq!(res.predicate.as_str(), "muche");
  assert_eq!(res.object, Term::literal_schema("wumpus", "schema"));
}

#[test]
fn test_parse_triple_invalid_line() {
  let res = parse_triple(r#"<hackem> <muche> "wumpus"#);
  assert!(res.is_err());
}
