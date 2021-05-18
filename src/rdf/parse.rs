//! Parse RDF N-triples.
//!
//! This module contains a parser for the [RDF N-triple format][nt], so we don't
//! have to pull in all of Oxigraph, can be more robust to bad data formats, and
//! improve parsing performnce.  Why do we need PEG to parse n-triples?
//!
//! [nt]: https://www.w3.org/TR/n-triples/
use std::convert::Into;
use std::str::FromStr;
use std::borrow::Cow;

use regex::{Regex, Captures};
use lazy_static::lazy_static;
use thiserror::Error;

use super::model::*;

#[derive(Error, Debug)]
pub enum ParseError {
  #[error("Invalid IRI reference: {0}")]
  BadIRI(String),
  #[error("Line is malformed: {0}")]
  MalformedLine(String),
  #[error("Trailing garbage after valid tuple: {0}")]
  TrailingGarbage(String),
}

lazy_static! {
  // Whitespace RE
  static ref WS_RE: Regex = Regex::new(r"\s+").expect("invalid regex");
  // Unicode escape RE
  static ref UESC_RE: Regex = Regex::new(
    r"\\u(?P<x4>[[:xdigit:]]{4})|\\U(?P<x8>[[:xdigit:]]{8})"
  ).expect("invalid regex");
  // General escape RE
  static ref ESC_RE: Regex = Regex::new(
    r#"\\(?P<c>[tbnrf"'\\])|\\u(?P<x4>[[:xdigit:]]{4})|\\U(?P<x8>[[:xdigit:]]{8})"#
  ).expect("invalid regex");

  // Comments and whitespace RE
  static ref EMPTY_RE: Regex = Regex::new(
    r#"^\s*(#.*)?$"#
  ).expect("invalid regex");

  // Triple parsing RE
  static ref TRIPLE_RE: Regex = re_triple();
}

const UCHAR: &'static str = r"\\u[[:xdigit:]]{4}|\\U[[:xdigit:]]{8}";
const ECHAR: &'static str = r#"\\[tbnrf"'\\]"#;

/// Regex component for a blank node.
fn re_blank() -> String {
  let pncu_parts = [
    r"A-Z",
    r"a-z",
    r"\u00C0-\u00D6",
    r"\u00D8-\u00F6",
    r"\u00F8-\u02FF",
    r"\u0370-\u037D",
    r"\u037F-\u1FFF",
    r"\u200C-\u200D",
    r"\u2070-\u218F",
    r"\u2C00-\u2FEF",
    r"\u3001-\uD7FF",
    r"\uF900-\uFDCF",
    r"\uFDF0-\uFFFD",
    r"\u{10000}-\u{EFFFF}",
    r"_:",
    r"0-9",
  ];
  let pnc_parts = [
    r"\u0300-\u036F",
    r"\u203F-\u2040",
    r"\u00B7",
  ];
  format!(
    r"_:[{pncu}](?:[{pncu}{pnc}.-]*[{pncu}{pnc}-])?",
    pncu=pncu_parts.join(""),
    pnc=pnc_parts.join(""),
  )
}

/// Regex component for an IRI reference.
fn re_iriref() -> String {
  format!(r#"<(?:[^\x00-\x20<>"{{}}|^`\\]|{uchar})*>"#, uchar=UCHAR)
}

/// Regex component for a string literal
fn re_stringlit() -> String {
  format!(r#""([^\x22\x5C\x0A\x0D]|{echar}|{uchar})*""#, uchar=UCHAR, echar=ECHAR)
}

/// Regex component for a language tag, matched in named group `lang`.
fn re_langtag() -> String {
  r#"@(?P<lang>[a-zA-Z]+(?:-[a-zA-Z0-9]+)*)"#.to_string()
}

/// Regex component for a literal; the schema and language are in named groups.
fn re_literal() -> String {
  format!(
    r#"(?P<lit_val>{slit})(?:\^\^(?P<schema>{iri})|{lang})?"#,
    slit=re_stringlit(),
    iri=re_iriref(),
    lang=re_langtag(),
  )
}

/// Compile a regex for a triple.
fn re_triple() -> Regex {
  let subj = format!(
    r#"(?P<subj>{iri}|{blank})"#,
    iri=re_iriref(),
    blank=re_blank()
  );
  let pred = format!(
    r#"(?P<pred>{iri})"#,
    iri=re_iriref()
  );
  let obj = format!(
    r#"(?P<obj>{iri}|{blank}|{lit})"#,
    iri=re_iriref(),
    blank=re_blank(),
    lit=re_literal()
  );
  let triple = format!(
    r#"^[ \t]*{subj}[ \t]+{pred}[ \t]+{obj}[ \t]+."#,
    subj=subj, pred=pred, obj=obj
  );
  Regex::new(&triple).expect("invalid regex")
}

fn decode_uescape(caps: &Captures<'_>) -> String {
  let x4 = caps.name("x4");
  let x8 = caps.name("x8");
  let xs = x4.or(x8).unwrap();
  let cp: u32 = u32::from_str_radix(xs.as_str(), 16).expect("invalid integer");
  let ch = char::from_u32(cp).unwrap();  // better error handling for escapes
  let mut buf = [0 as u8; 4];
  ch.encode_utf8(&mut buf).to_owned()
}

fn decode_escape(caps: &Captures<'_>) -> Cow<'static, str> {
  if let Some(c) = caps.name("c") {
    let s = match c.as_str() {
      "t" => "\t",
      "b" => "\x07",
      "n" => "\n",
      "r" => "\r",
      "f" => "\x12",
      "\"" => "\"",
      "'" => "'",
      "\\" => "\\",
      _ => unreachable!("invalid escape character")
    };
    s.into()
  } else {
    decode_uescape(caps).into()
  }
}

/// Decode an IRI and replace escape characters.
fn decode_iri<'a>(iri: &'a str) -> Cow<'a, str> {
  // if this happens, we passed something not recognized as an IRI
  let iri_bs = iri.as_bytes();
  assert_eq!(iri_bs[0], b'<');
  assert_eq!(iri_bs[iri_bs.len() - 1], b'>');
  // trim off the delimiters
  let iri = &iri[1..(iri.len() - 1)];
  // and let's replace!
  UESC_RE.replace_all(iri, decode_uescape)
}

/// Decode a blank node - this just returns the key
fn decode_blank<'a>(blank: &'a str) -> Cow<'a, str> {
  assert_eq!(&blank[0..2], "_:");
  blank[2..].into()
}

/// Decode a string literal and replace escape characters
fn decode_lit<'a>(lit: &'a str) -> Cow<'a, str> {
  // if this happens, we passed something not recognized as an IRI
  let lit_bs = lit.as_bytes();
  assert_eq!(lit_bs[0], b'"');
  assert_eq!(lit_bs[lit_bs.len() - 1], b'"');
  // trim off the delimiters
  let lit = &lit[1..(lit.len() - 1)];
  // and let's replace!
  ESC_RE.replace_all(lit, decode_escape)
}

/// Decode a node
fn decode_node<'a>(subj: &'a str) -> Node {
  let first = subj.as_bytes()[0];
  match first {
    b'<' => Node::named(decode_iri(subj)),
    b'_' => Node::blank(decode_blank(subj)),
    _ => unreachable!("bad subject")
  }
}

/// Parse a triple
pub fn parse_triple(line: &str) -> Result<Option<Triple>, ParseError> {
  // try to parse a triple
  let (trip, rest) = if let Some(caps) = TRIPLE_RE.captures(line) {
    let subj = caps.name("subj").unwrap();
    let subj = decode_node(subj.as_str());

    let pred = caps.name("pred").unwrap();
    let pred = decode_iri(pred.as_str()).into_owned();

    let obj = if let Some(lv) = caps.name("lit_val") {
      // object is a literal value
      let value = decode_lit(lv.as_str()).into_owned();

      let lang = caps.name("lang");
      let lang = lang.map(|s| s.as_str().to_string());

      let schema = caps.name("schema");
      let schema = schema.map(|s| decode_iri(s.as_str()).into_owned());

      Term::Literal(Literal {
        value, lang, schema
      })
    } else {
      // it's a node - grab obj (always exists) and decode
      Term::Node(decode_node(caps.name("obj").unwrap().as_str()))
    };
    (Some(Triple {
      subject: subj,
      predicate: pred,
      object: obj
    }), &line[caps.get(0).unwrap().end()..])
  } else {
    (None, line)
  };

  if EMPTY_RE.is_match(rest) {
    // everything is fine, all that's left is whitespace and maybe a comment
    Ok(trip)
  } else if trip.is_none() {
    Err(ParseError::MalformedLine(line.to_owned()))
  } else {
    Err(ParseError::TrailingGarbage(rest.to_owned()))
  }
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

#[test]
fn test_iri_decode_empty() {
  let res = decode_iri("<>");
  assert_eq!(res.as_ref(), "");
}

#[test]
fn test_iri_decode_url() {
  let res = decode_iri("<https://example.com>");
  assert_eq!(res.as_ref(), "https://example.com");
}

#[test]
fn test_iri_decode_escape() {
  let res = decode_iri("<https://example.com/\\u2665>");
  assert_eq!(res.as_ref(), "https://example.com/♥");
}

#[test]
fn test_iri_decode_escapes() {
  let res = decode_iri("<https://example.com/\\u2665/\\U0001F49E>");
  assert_eq!(res.as_ref(), "https://example.com/♥/💞");
}

#[test]
fn test_lit_decode_empty() {
  let res = decode_lit("\"\"");
  assert_eq!(res.as_ref(), "");
}

#[test]
fn test_lit_decode_url() {
  let res = decode_lit("\"hello\"");
  assert_eq!(res.as_ref(), "hello");
}

#[test]
fn test_lit_decode_escape() {
  let res = decode_lit("\"HACKEM \\u2665 MUCHE\"");
  assert_eq!(res.as_ref(), "HACKEM ♥ MUCHE");
}

#[test]
fn test_lit_decode_escapes() {
  let res = decode_lit("\"FOOBIE \\u2665 BLETCH \\U0001F49E\"");
  assert_eq!(res.as_ref(), "FOOBIE ♥ BLETCH 💞");
}

#[test]
fn test_lit_decode_cescape() {
  let res = decode_lit("\"FOOBIE \\r BLETCH\"");
  assert_eq!(res.as_ref(), "FOOBIE \r BLETCH");
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
fn test_re_langtag() {
  let re = Regex::new(re_langtag().as_str()).expect("invalid RE");
  assert!(re.is_match("@en-US"));
  assert!(!re.is_match("foo"));
}

#[test]
fn test_re_iri() {
  let re = Regex::new(re_iriref().as_str()).expect("invalid RE");
  assert!(re.is_match("<https://example.com>"));
  assert!(!re.is_match("wombat"));
  assert!(!re.is_match("_:blank"));
}

#[test]
fn test_re_blank() {
  let re = Regex::new(re_blank().as_str()).expect("invalid RE");
  assert!(re.is_match("_:dingbat"));
  assert!(!re.is_match("wombat"));
  assert!(!re.is_match("_:\txx"));
}

#[test]
fn test_re_string() {
  let re = Regex::new(re_stringlit().as_str()).expect("invalid RE");
  assert!(re.is_match(r#""fish""#));
  assert!(!re.is_match("wombat"));
  assert!(!re.is_match("<foo>"));
}

#[test]
fn test_re_literal_string() {
  let re = Regex::new(re_literal().as_str()).expect("invalid RE");
  let m = re.captures(r#""fish""#);
  assert!(m.is_some());
  let m = m.unwrap();
  assert_eq!(m.name("lit_val").unwrap().as_str(), r#""fish""#);
  assert!(m.name("lang").is_none());
  assert!(m.name("schema").is_none());

  assert!(!re.is_match("wombat"));
  assert!(!re.is_match("<foo>"));
}

#[test]
fn test_re_literal_string_lang() {
  let re = Regex::new(re_literal().as_str()).expect("invalid RE");
  let m = re.captures(r#""fish"@en"#);
  assert!(m.is_some());
  let m = m.unwrap();
  assert_eq!(m.name("lit_val").unwrap().as_str(), r#""fish""#);
  assert_eq!(m.name("lang").unwrap().as_str(), "en");
  assert!(m.name("schema").is_none());

  assert!(!re.is_match("wombat"));
  assert!(!re.is_match("<foo>"));
}

#[test]
fn test_re_literal_string_schema() {
  let re = Regex::new(re_literal().as_str()).expect("invalid RE");
  println!("compiled RE: {:?}", re);
  let m = re.captures(r#""fish"^^<foo>"#);
  assert!(m.is_some());
  let m = m.unwrap();
  assert_eq!(m.name("lit_val").unwrap().as_str(), r#""fish""#);
  assert!(m.name("lang").is_none());
  assert_eq!(m.name("schema").unwrap().as_str(), "<foo>");

  assert!(!re.is_match("wombat"));
  assert!(!re.is_match("<foo>"));
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
