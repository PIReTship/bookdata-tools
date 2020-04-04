use anyhow::Result;
use regex::{Regex, RegexSet, Match, Captures};

/// Single ISBN parsed from a string
#[derive(Debug, PartialEq)]
pub struct ISBN {
  pub text: String,
  pub tags: Vec<String>
}

/// Result of parsing an ISBN string
#[derive(Debug, PartialEq)]
pub enum ParseResult {
  Valid(Vec<ISBN>, String),
  Ignored(String),
  Unmatched(String)
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
  lead: Regex,
  isbn: Regex,
  tag: Regex,
  tail_skip: Regex,
  clean: Regex,
  unmatch_ignore: RegexSet
}

impl ParserDefs {
  pub fn new() -> ParserDefs {
    fn cre(p: &str) -> Regex {
      Regex::new(p).unwrap()
    }

    // we use unwrap instead of result since regex compile failure is a programming error
    ParserDefs {
      lead: cre(r"^\s*(?:[a-z]\s+|\(\d+\)\s+|\*)?"),
      isbn: cre(r"^([0-9-]{8,}[Xx]?|[0-9]{1,5}(?:[a-zA-Z]+|[ +])[0-9-]{4,})"),
      tag: cre(r"^(?:\s*\((.+?)\))?"),
      tail_skip: cre(r"^\s*[;:/.]?"),

      clean: cre(r"[a-wyzA-WYZ -]"),
      unmatch_ignore: RegexSet::new(IGNORES).unwrap()
    }
  }

  /// Create a new parser to parse a string.
  pub fn create_parser<'p>(&'p self, s: &str) -> IsbnParser<'p> {
    IsbnParser {
      defs: self,
      string: preclean(s),
      position: 0
    }
  }

  /// Parse a string
  pub fn parse(&self, s: &str) -> ParseResult {
    let parser = self.create_parser(s);
    // FIXME implement this!
    ParseResult::Ignored(s.to_owned())
  }
}

pub struct IsbnParser<'p> {
  defs: &'p ParserDefs,
  string: String,
  position: usize
}

fn preclean(s: &str) -> String {
  let mut res = String::with_capacity(s.len());
  for c in s.chars() {
    if c.is_ascii() {
      res.push(c);
    }
  }
  res
}

impl <'p> IsbnParser<'p> {
  /// Get the remaining (unparsed) text from the parser
  fn remaining<'s>(&'s self) -> &'s str {
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
  fn peek(&self, rex: &Regex) -> Option<Match> {
    let slice = self.remaining();
    rex.find(slice)
  }

  /// See if a regex matches, and advance if it does.
  fn read(&mut self, rex: &Regex) -> Option<Match> {
    let slice = self.remaining();
    let res = rex.find(slice);
    if let Some(m) = res {
      //self.advance(m.end());
    }
    res
  }

  /// Advance the parse position by `n` characters.
  fn advance(&mut self, n: usize) {
    self.position += n;
  }

  fn is_empty(&self) -> bool {
    self.position == self.string.len()
  }

  /// Read a single ISBN
  fn read_isbn(&mut self) -> Option<ISBN> {
    self.eat(&self.defs.lead);
    self.read(&self.defs.isbn).map(|m| ISBN {
      text: m.as_str().to_owned(),
      tags: Vec::new()
    })
  }

  // fn scan<'a>(&self, text: &'a str) -> Option<(ISBN, usize)> {
  //   for (rex, func) in &self.parsers {
  //     if let Some(cap) = rex.captures(text) {
  //       if let Some(res) = func(&cap) {
  //         let all = cap.get(0).unwrap();
  //         return Some((res, all.end()))
  //       }
  //     }
  //   }
  //   return None
  // }

  // pub fn parse<'a>(&self, text: &'a str) -> ParseResult {
  //   let text = preclean(text);
  //   let mut isbns = Vec::new();
  //   let mut haystack = text.as_str();
  //   while let Some((res, end)) = self.scan(haystack) {
  //     isbns.push(res);
  //     haystack = &haystack[end..];
  //   }

  //   if isbns.is_empty() {
  //     for rex in &self.ignores {
  //       if rex.is_match(&text) {
  //         return ParseResult::Ignored(text.to_owned())
  //       }
  //     }
  //     ParseResult::Unmatched(text.to_owned())
  //   } else {
  //     ParseResult::Valid(isbns, haystack.to_owned())
  //   }
  // }
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
  assert_eq!(parser.string.as_str(), target);
  assert_eq!(parser.remaining(), "jimbob");
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
  let src = "349̌224010X";
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
