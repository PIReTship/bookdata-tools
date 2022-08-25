//! Code for cleaning up ISBNs.
//!
//! This module contains two families of functions:
//!
//! - The simple character-cleaning functions [clean_isbn_chars] and [clean_asin_chars].
//! - The full multi-ISBN parser [ParserDefs].
//!
//! When a string is a relatively well-formed ISBN (or ASIN), the character-cleaning functions
//! are fine.  Some sources, however (such as the Library of Congress) have messy ISBNs that
//! may have multiple ISBNs in one string, descriptive tags, and all manner of other messes.
//! The multi-ISBN parser exposed through [ParserDefs] supports cleaning these ISBN strings.
use regex::{Regex, RegexSet, Match, Captures};

use crate::util::unicode::NONSPACING_MARK;

/// Single ISBN parsed from a string.
#[derive(Debug, PartialEq)]
pub struct ISBN {
  pub text: String,
  pub tags: Vec<String>
}

/// Result of parsing an ISBN string.
#[derive(Debug, PartialEq)]
pub enum ParseResult {
  /// Vaid ISBNs, possibly with additional trailing text
  Valid(Vec<ISBN>, String),
  /// An ignored string
  Ignored(String),
  /// An unparsable string
  Unmatched(String)
}

peg::parser! {
  grammar isbn_parser() for str {
    rule space() = quiet!{[' ' | '\n' | '\r' | '\t']}

    rule lead() =
      [';' | '.']? space()* prefix()?
    rule prefix()
      = ['a'..='z'] space()+
      / "(" ['0'..='9']+ ")" space()+
      / "*"
      / "ISBN" space()+

    rule single_tag() -> String = s:$([^ ':' | ')' | ']']+) { s.trim().to_string() }

    rule tags() -> Vec<String>
      = space()* ['[' | '('] tags:(single_tag() ** ":") [']' | ')'] { tags }

    rule tail_skip() = space()* [';' | ':' | '/' | '.']?

    // digits, hyphens, and misc. marks are allowed (will clean later)
    rule digit_char() -> char
      = mc:['0'..='9' | '-'] { mc }
      / mc:[c if NONSPACING_MARK.contains(c)] { mc }

    // some ISBNs have some random junk in the middle, match allowed junk
    rule inner_junk() = ['a'..='a' | 'A'..='Z']+ / [' ' | '+']

    rule isbn_text() -> String
      = s:$(digit_char()*<8,> ['X' | 'x']?) { clean_isbn_chars(s) }
      / s:$(['0'..='9']*<1,5> inner_junk() ['0'..='9' | '-']*<4,>) { clean_isbn_chars(s) }

    rule isbn() -> ISBN
      = lead() i:isbn_text() tags:tags()* { ISBN {
        text: i,
        tags: tags.into_iter().flatten().collect()
    }}

    pub rule parse_isbns() -> (Vec<ISBN>, String)
      = v:(isbn() ** tail_skip()) tail_skip() t:$([_]*) { (v, t.into()) }
  }
}

/// Crude ISBN cleanup.
///
/// To be used with ISBN strings that are relatively well-formed, but may have invalid
/// characters.
pub fn clean_isbn_chars(isbn: &str) -> String {
  let mut vec = Vec::with_capacity(isbn.len());
  let bytes = isbn.as_bytes();
  for i in 0..bytes.len() {
    let b = bytes[i];
    if b.is_ascii_digit() || b == b'X' {
      vec.push(b)
    } else if b == b'x' {
      vec.push(b'X')
    }
  }
  unsafe {  // we know it's only ascii
    String::from_utf8_unchecked(vec)
  }
}

/// Crude ASIN cleanup.
pub fn clean_asin_chars(isbn: &str) -> String {
  let mut vec = Vec::with_capacity(isbn.len());
  let bytes = isbn.as_bytes();
  for i in 0..bytes.len() {
    let b = bytes[i];
    if b.is_ascii_alphanumeric() {
      vec.push(b.to_ascii_uppercase())
    }
  }
  unsafe {  // we know it's only ascii
    String::from_utf8_unchecked(vec)
  }
}

/// Regular expressions for unparsable ISBN strings to ignore.
/// This cleans up warning displays.
static IGNORES: &'static [&'static str] = &[
  r"^[$]?[[:digit:]., ]+(?:[a-zA-Z*]{1,4})?(\s+\(.*?\))?$",
  r"^[[:digit:].]+(/[[:digit:].]+)+$",
  r"^[A-Z]-[A-Z]-\d{8,}",
  r"^\s*$"
];

/// Definitions for parsing ISBN strings.
pub struct ParserDefs {
  /// Matcher for text that may appear before an ISBN
  lead: Regex,
  /// Matcher for a single ISBN
  isbn: Regex,
  /// Matcher for a "tag" after an ISBN
  tag: Regex,
  /// Matcher for separarators between multiple tags
  tag_sep: Regex,
  /// Matcher for text to skip before possibly reading another ISBN
  tail_skip: Regex,
  /// Matcher for characters to remove from a parsed ISBN
  clean: Regex,
  /// Matcher for text that is known not to contain any parseable ISBNs
  unmatch_ignore: RegexSet
}

impl ParserDefs {
  /// Create a new set of parser definitions.
  pub fn new() -> ParserDefs {
    fn cre(p: &str) -> Regex {
      // we use unwrap instead of result since regex compile failure is a programming error
      Regex::new(p).unwrap()
    }

    ParserDefs {
      lead: cre(r"^[;.]?\s*(?:[a-z]\s+|\(\d+\)\s+|\*|ISBN\s+)?"),
      isbn: cre(r"^([\p{Nonspacing Mark}0-9-]{8,}[Xx]?|[0-9]{1,5}(?:[a-zA-Z]+|[ +])[0-9-]{4,})"),
      tag: cre(r"^\s*[(\[](.+?)[)\]]"),
      tag_sep: cre(r"\s*:\s*"),
      tail_skip: cre(r"^\s*[;:/.]?"),

      clean: cre(r"[\p{Nonspacing Mark}a-wyzA-WYZ +-]"),
      unmatch_ignore: RegexSet::new(IGNORES).unwrap()
    }
  }

  /// Create a new parser to incrementally parse a string.
  fn create_parser<'p, 's>(&'p self, s: &'s str) -> IsbnParser<'p, 's> {
    IsbnParser {
      defs: self,
      string: s,
      position: 0
    }
  }

  /// Parse an ISBN string.
  pub fn parse(&self, s: &str) -> ParseResult {
    // let mut parser = self.create_parser(s);
    // parser.read_all()
    if let Ok((isbns, tail)) = isbn_parser::parse_isbns(s) {
      if isbns.is_empty() {
        if self.unmatch_ignore.is_match(s) {
          ParseResult::Ignored(s.to_owned())
        } else {
          ParseResult::Unmatched(s.to_owned())
        }
      } else {
        ParseResult::Valid(isbns, tail)
      }
    } else {
      ParseResult::Unmatched(s.to_owned())
    }
  }
}

/// State for parsing a single ISBN string.
///
/// This struct does not publicly expose any data or operations.
struct IsbnParser<'p, 's> {
  defs: &'p ParserDefs,
  string: &'s str,
  position: usize
}

#[allow(unused)]
fn preclean(s: &str) -> String {
  let mut res = String::with_capacity(s.len());
  for c in s.chars() {
    if c.is_ascii() {
      res.push(c);
    }
  }
  res
}

impl <'p, 's> IsbnParser<'p, 's> {
  /// Get the remaining (unparsed) text from the parser
  fn remaining(&self) -> &'s str {
    &self.string[self.position..]
  }

  /// Advance the parse position to the end of a regex patch, if possible.
  fn eat(&mut self, rex: &Regex) -> bool {
    let slice = self.remaining();
    if let Some(m) = rex.find(slice) {
      self.advance(m.end());
      true
    } else {
      false
    }
  }

  /// See if a regex matches.
  #[allow(dead_code)]
  fn peek(&self, rex: &Regex) -> Option<Match> {
    let slice = self.remaining();
    rex.find(slice)
  }

  /// See if a regex matches, and advance if it does.
  fn read(&mut self, rex: &Regex) -> Option<Match<'s>> {
    let slice = self.remaining();
    let res = rex.find(slice);
    if let Some(m) = res {
      self.advance(m.end());
    }
    res
  }

  /// Read with capture groups
  fn read_cap(&mut self, rex: &Regex) -> Option<Captures<'s>> {
    let slice = self.remaining();
    let res = rex.captures(slice);
    if let Some(ref m) = res {
      self.advance(m.get(0).unwrap().end());
    }
    res
  }

  /// Advance the parse position by `n` characters.
  fn advance(&mut self, n: usize) {
    self.position += n;
  }

  #[cfg(test)]
  fn is_empty(&self) -> bool {
    self.position == self.string.len()
  }

  /// Read a single ISBN
  fn read_isbn(&mut self) -> Option<ISBN> {
    self.eat(&self.defs.lead);
    self.read(&self.defs.isbn).map(|m| ISBN {
      text: self.defs.clean.replace_all(m.as_str(), "").to_uppercase(),
      tags: self.read_tags()
    })
  }

  /// Read tags (assuming an ISBN has just been read)
  fn read_tags(&mut self) -> Vec<String> {
    let mut tags = Vec::new();
    while let Some(m) = self.read_cap(&self.defs.tag) {
      let tag = m.get(1).unwrap().as_str();
      for t in self.defs.tag_sep.split(tag) {
        tags.push(t.to_owned());
      }
    }
    tags
  }

  /// Read all ISBNs
  fn read_all(&mut self) -> ParseResult {
    let mut isbns = Vec::new();
    while let Some(res) = self.read_isbn() {
      isbns.push(res);
      // advance through our skip
      self.eat(&self.defs.tail_skip);
    }

    if isbns.is_empty() {
      if self.defs.unmatch_ignore.is_match(self.string) {
        ParseResult::Ignored(self.string.to_owned())
      } else {
        ParseResult::Unmatched(self.string.to_owned())
      }
    } else {
      ParseResult::Valid(isbns, self.remaining().to_owned())
    }
  }
}

#[test]
fn test_preclean_keep() {
  assert_eq!(preclean("foo").as_str(), "foo");
}

#[test]
fn test_preclean_caron() {
  let src = "349̌224010X";
  let isbn = "349224010X";
  assert_eq!(preclean(src).as_str(), isbn);
}

#[test]
fn test_parser_initial() {
  let defs = ParserDefs::new();
  let target = "jimbob";
  let parser = defs.create_parser(target);
  assert_eq!(parser.position, 0);
  assert_eq!(parser.string, target);
  assert_eq!(parser.remaining(), target);
}

#[test]
fn test_eat_nomatch() {
  let defs = ParserDefs::new();
  let target = "jimbob";
  let pat = Regex::new(r"^\d").unwrap();
  let mut parser = defs.create_parser(target);
  assert!(!parser.eat(&pat));
  assert_eq!(parser.position, 0);
}

#[test]
fn test_eat_match() {
  let defs = ParserDefs::new();
  let target = "jimbob";
  let pat = Regex::new(r"^jim").unwrap();
  let mut parser = defs.create_parser(target);
  assert!(parser.eat(&pat));
  assert_eq!(parser.position, 3);
  assert!(!parser.is_empty());
  assert_eq!(parser.remaining(), "bob");
}

#[test]
fn test_eat_later() {
  let defs = ParserDefs::new();
  let target = "jimjim";
  let pat = Regex::new(r"^jim").unwrap();
  let mut parser = defs.create_parser(target);
  assert!(parser.eat(&pat));
  assert_eq!(parser.position, 3);
  assert!(parser.eat(&pat));
  assert_eq!(parser.position, 6);
  assert!(parser.is_empty());
  // eating again fails
  assert!(!parser.eat(&pat));
  assert_eq!(parser.remaining(), "");
}

#[test]
fn test_scan_empty() {
  let defs = ParserDefs::new();
  let mut parser = defs.create_parser("");
  assert_eq!(parser.read_isbn(), None);
}

#[test]
fn test_parse_empty() {
  let defs = ParserDefs::new();
  let res = defs.parse("");
  assert_eq!(res, ParseResult::Ignored("".to_owned()));
}

#[test]
fn test_scan_ws() {
  let defs = ParserDefs::new();
  let mut parser = defs.create_parser("  ");
  assert_eq!(parser.read_isbn(), None);
}

#[test]
fn test_parse_ws() {
  let defs = ParserDefs::new();
  let res = defs.parse("  ");
  assert_eq!(res, ParseResult::Ignored("  ".to_owned()));
}

#[test]
fn test_parse_isbn() {
  let isbn = "349224010X";
  let defs = ParserDefs::new();

  let mut parser = defs.create_parser(isbn);
  let scan = parser.read_isbn();
  assert!(scan.is_some());
  let scan = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.tags.len(), 0);
  assert_eq!(parser.position, isbn.len());
  assert!(parser.is_empty());

  let res = defs.parse(isbn);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].tags.len(), 0);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_isbn_trail() {
  let src = "349224010X :";
  let isbn = "349224010X";
  let defs = ParserDefs::new();

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].tags.len(), 0);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_scan_caron() {
  // this string has a combining mark (caron, unicode 730) in it
  let src = "349̌224010X";
  // we want a cleaned ISBN
  let isbn = "349224010X";
  let defs = ParserDefs::new();
  let mut parser = defs.create_parser(src);
  let res = parser.read_isbn().unwrap();
  assert_eq!(res.text, isbn);
}

#[test]
fn test_parse_isbn_caron() {
  let src = "349̌224010X";
  let isbn = "349224010X";
  let defs = ParserDefs::new();

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].tags.len(), 0);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_hyphen_isbn() {
  let src = "978-03-2948-9391";
  let isbn = "9780329489391";
  let defs = ParserDefs::new();

  let mut parser = defs.create_parser(src);
  let scan = parser.read_isbn();
  assert!(scan.is_some());
  let scan = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.tags.len(), 0);
  assert!(parser.is_empty());

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].tags.len(), 0);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_space_isbn() {
  let src = "978 032948-9391";
  let isbn = "9780329489391";
  let defs = ParserDefs::new();

  let mut parser = defs.create_parser(src);
  let scan = parser.read_isbn();
  assert!(scan.is_some());
  let scan = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.tags.len(), 0);
  assert!(parser.is_empty());

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].tags.len(), 0);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_isbn_tag() {
  let src = "34922401038 (set)";
  let isbn = "34922401038";
  let tag = "set";
  let defs = ParserDefs::new();

  let mut parser = defs.create_parser(src);
  let scan = parser.read_isbn();
  assert!(scan.is_some());
  let scan = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.tags, vec![tag]);
  assert!(parser.is_empty());

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].tags, vec![tag]);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_isbn_square_tag() {
  let src = "34922401038 [set]";
  let isbn = "34922401038";
  let tag = "set";
  let defs = ParserDefs::new();

  let mut parser = defs.create_parser(src);
  let scan = parser.read_isbn();
  assert!(scan.is_some());
  let scan = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.tags, vec![tag]);
  assert!(parser.is_empty());

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].tags, vec![tag]);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_isbn_multi_tag_sep() {
  let src = "34922401038 (set : alk. paper)";
  let isbn = "34922401038";
  let defs = ParserDefs::new();

  let mut parser = defs.create_parser(src);
  let scan = parser.read_isbn();
  assert!(scan.is_some());
  let scan = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.tags, vec!["set", "alk. paper"]);
  assert!(parser.is_empty());

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].tags, vec!["set", "alk. paper"]);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}


#[test]
fn test_parse_isbn_tags() {
  let src = "34922401038 (pbk.) (set)";
  let isbn = "34922401038";
  let defs = ParserDefs::new();

  let mut parser = defs.create_parser(src);
  let scan = parser.read_isbn();
  assert!(scan.is_some());
  let scan = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.tags, vec!["pbk.", "set"]);
  assert!(parser.is_empty());

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].tags, vec!["pbk.", "set"]);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_isbn_leader() {
  let src = "a 970238408138";
  let isbn = "970238408138";
  let defs = ParserDefs::new();

  let mut parser = defs.create_parser(src);
  let scan = parser.read_isbn();
  assert!(scan.is_some());
  let scan = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.tags.len(), 0);
  assert!(parser.is_empty());

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].tags.len(), 0);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_two_isbns_ws() {
  let src = "970238408138 30148100103";
  let isbn1 = "970238408138";
  let isbn2 = "30148100103";
  let defs = ParserDefs::new();

  let mut parser = defs.create_parser(src);
  let scan = parser.read_isbn();
  assert!(scan.is_some());
  let scan = scan.unwrap();
  assert_eq!(scan.text, isbn1);
  assert_eq!(scan.tags.len(), 0);
  assert_eq!(parser.position, isbn1.len());

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 2);
      assert_eq!(isbns[0].text, isbn1);
      assert_eq!(isbns[0].tags.len(), 0);
      assert_eq!(isbns[1].text, isbn2);
      assert_eq!(isbns[1].tags.len(), 0);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_two_isbns_semi() {
  let src = "970238408138; ISBN 30148100103";
  let isbn1 = "970238408138";
  let isbn2 = "30148100103";
  let defs = ParserDefs::new();

  let mut parser = defs.create_parser(src);
  let scan = parser.read_isbn();
  assert!(scan.is_some());
  let scan = scan.unwrap();
  assert_eq!(scan.text, isbn1);
  assert_eq!(scan.tags.len(), 0);
  assert_eq!(parser.position, isbn1.len());

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 2);
      assert_eq!(isbns[0].text, isbn1);
      assert_eq!(isbns[0].tags.len(), 0);
      assert_eq!(isbns[1].text, isbn2);
      assert_eq!(isbns[1].tags.len(), 0);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}


#[test]
fn test_parse_two_isbns_real() {
  // Real example from record 2175696
  let src = "8719359022. ISBN 8719359004 (pbk.)";
  let isbn1 = "8719359022";
  let isbn2 = "8719359004";
  let defs = ParserDefs::new();

  let mut parser = defs.create_parser(src);
  let scan = parser.read_isbn();
  assert!(scan.is_some());
  let scan = scan.unwrap();
  assert_eq!(scan.text, isbn1);
  assert_eq!(scan.tags.len(), 0);

  let res = defs.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 2);
      assert_eq!(isbns[0].text, isbn1);
      assert_eq!(isbns[0].tags.len(), 0);
      assert_eq!(isbns[1].text, isbn2);
      assert_eq!(isbns[1].tags, vec!["pbk."]);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}
