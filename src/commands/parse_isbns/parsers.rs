use lazy_static::lazy_static;
use anyhow::Result;
use regex::{Regex, Captures};
use std::str::FromStr;

#[derive(Debug, PartialEq)]
pub struct ISBN {
  pub text: String,
  pub descriptor: Option<String>
}

lazy_static! {
  static ref CLEANER: Regex= Regex::from_str("[a-wyzA-WYZ -]").unwrap();
}

fn parse_plain<'t>(m: &Captures<'t>) -> Option<ISBN> {
  let isbn = m.get(1).unwrap().as_str().trim();
  if isbn.len() > 0 {
    let desc = m.get(2).map(|m| m.as_str().to_owned());
    Some(ISBN {
      text: CLEANER.replace_all(isbn, "").to_string(),
      descriptor: desc
    })
  } else {
    None
  }
}

static PARSERS: &'static [(&'static str, fn(&Captures) -> Option<ISBN>)] = &[
  (concat!(/* lead */ r"^\s*(?:[a-z]\s+|\(\d+\)\s+|\*)?",
           /* isbn */ r"([0-9-]{8,}[Xx]?)",
           /* tag  */ r"(?:\s*\((.+?)\))?",
           /* tail */ r"\s*[;:/.]?"),
   parse_plain),
  (concat!(/* lead */ r"^\s*(?:[a-z]\s+|\(\d+\)\s+|\*)?",
           /* isbn */ r"([0-9]{1,5}(?:[a-zA-Z]+|[ +])[0-9-]{4,})",
           /* tag  */ r"(?:\s*\((.+?)\))?",
           /* tail */ r"\s*[;:/.]?"),
   parse_plain),
  (concat!(/* lead */ r"^[;.]?\s*ISBN\s+",
           /* isbn */ r"([0-9-]{8,}[Xx]?)",
           /* tag  */ r"(?:\s*\((.+?)\))?",
           /* tail */ r"\s*[;:/.]?"),
   parse_plain)
];

static IGNORES: &'static [&'static str] = &[
  r"^[$]?[[:digit:]., ]+(?:[a-zA-Z*]{1,4})?(\s+\(.*?\))?$",
  r"^[[:digit:].]+(/[[:digit:].]+)+$",
  r"^[A-Z]-[A-Z]-\d{8,}",
  r"^\s*$"
];

pub struct IsbnParser {
  parsers: Vec<(Regex, &'static fn(&Captures) -> Option<ISBN>)>,
  ignores: Vec<Regex>
}

#[derive(Debug, PartialEq)]
pub enum ParseResult {
  Valid(Vec<ISBN>, String),
  Ignored(String),
  Unmatched(String)
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

impl IsbnParser {
  pub fn compile() -> Result<IsbnParser> {
    let mut compiled = Vec::with_capacity(PARSERS.len());
    for (pat, func) in PARSERS {
      let rex = Regex::new(pat)?;
      compiled.push((rex, func));
    }

    let mut comp_ignore = Vec::with_capacity(IGNORES.len());
    for pat in IGNORES {
      comp_ignore.push(Regex::new(pat)?);
    }

    Ok(IsbnParser {
      parsers: compiled,
      ignores: comp_ignore
    })
  }

  fn scan<'a>(&self, text: &'a str) -> Option<(ISBN, usize)> {
    for (rex, func) in &self.parsers {
      if let Some(cap) = rex.captures(text) {
        if let Some(res) = func(&cap) {
          let all = cap.get(0).unwrap();
          return Some((res, all.end()))
        }
      }
    }
    return None
  }

  pub fn parse<'a>(&self, text: &'a str) -> ParseResult {
    let text = preclean(text);
    let mut isbns = Vec::new();
    let mut haystack = text.as_str();
    while let Some((res, end)) = self.scan(haystack) {
      isbns.push(res);
      haystack = &haystack[end..];
    }

    if isbns.is_empty() {
      for rex in &self.ignores {
        if rex.is_match(&text) {
          return ParseResult::Ignored(text.to_owned())
        }
      }
      ParseResult::Unmatched(text.to_owned())
    } else {
      ParseResult::Valid(isbns, haystack.to_owned())
    }
  }
}

#[test]
fn test_preclean_keep() {
  assert_eq!(preclean("foo").as_str(), "foo");
}

#[test]
fn test_preclean_charon() {
  let src = "349̌224010X";
  let isbn = "349224010X";
  assert_eq!(preclean(src).as_str(), isbn);
}

#[test]
fn test_scan_empty() {
  let parsers = IsbnParser::compile().unwrap();
  assert_eq!(parsers.scan(""), None);
}

#[test]
fn test_parse_empty() {
  let parsers = IsbnParser::compile().unwrap();
  let res = parsers.parse("");
  assert_eq!(res, ParseResult::Ignored("".to_owned()));
}


#[test]
fn test_scan_ws() {
  let parsers = IsbnParser::compile().unwrap();
  assert_eq!(parsers.scan("  "), None);
}

#[test]
fn test_parse_ws() {
  let parsers = IsbnParser::compile().unwrap();
  let res = parsers.parse("  ");
  assert_eq!(res, ParseResult::Ignored("  ".to_owned()));
}

#[test]
fn test_parse_isbn() {
  let isbn = "349224010X";
  let parsers = IsbnParser::compile().unwrap();

  let (rex, parse) = &parsers.parsers[0];
  let m = rex.captures(isbn).unwrap();
  assert_eq!(m.get(1).unwrap().as_str(), isbn);
  let r = parse(&m).unwrap();
  assert_eq!(r.text, isbn);

  let scan = parsers.scan(isbn);
  assert!(scan.is_some());
  let (scan, end) = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.descriptor, None);
  assert_eq!(end, isbn.len());

  let res = parsers.parse(isbn);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].descriptor, None);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_isbn_trail() {
  let src = "349224010X :";
  let isbn = "349224010X";
  let parsers = IsbnParser::compile().unwrap();

  let res = parsers.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].descriptor, None);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_isbn_charon() {
  let src = "349̌224010X";
  let isbn = "349224010X";
  let parsers = IsbnParser::compile().unwrap();

  let res = parsers.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].descriptor, None);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_hyphen_isbn() {
  let src = "978-03-2948-9391";
  let isbn = "9780329489391";
  let parsers = IsbnParser::compile().unwrap();

  let scan = parsers.scan(src);
  assert!(scan.is_some());
  let (scan, end) = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.descriptor, None);
  assert_eq!(end, src.len());

  let res = parsers.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].descriptor, None);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}

#[test]
fn test_parse_space_isbn() {
  let src = "978 032948-9391";
  let isbn = "9780329489391";
  let parsers = IsbnParser::compile().unwrap();

  let scan = parsers.scan(src);
  assert!(scan.is_some());
  let (scan, end) = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.descriptor, None);
  assert_eq!(end, src.len());

  let res = parsers.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].descriptor, None);
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
  let parsers = IsbnParser::compile().unwrap();

  let scan = parsers.scan(src);
  assert!(scan.is_some());
  let (scan, end) = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.descriptor.as_ref().unwrap(), tag);
  assert_eq!(end, src.len());

  let res = parsers.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].descriptor.as_ref().unwrap(), tag);
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}


#[test]
fn test_parse_isbn_leader() {
  let src = "a 970238408138";
  let isbn = "970238408138";
  let parsers = IsbnParser::compile().unwrap();

  let scan = parsers.scan(src);
  assert!(scan.is_some());
  let (scan, end) = scan.unwrap();
  assert_eq!(scan.text, isbn);
  assert_eq!(scan.descriptor, None);
  assert_eq!(end, src.len());

  let res = parsers.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 1);
      assert_eq!(isbns[0].text, isbn);
      assert_eq!(isbns[0].descriptor, None);
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
  let parsers = IsbnParser::compile().unwrap();

  let scan = parsers.scan(src);
  assert!(scan.is_some());
  let (scan, end) = scan.unwrap();
  assert_eq!(scan.text, isbn1);
  assert_eq!(scan.descriptor, None);
  assert_eq!(end, isbn1.len() + 1);

  let res = parsers.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 2);
      assert_eq!(isbns[0].text, isbn1);
      assert_eq!(isbns[0].descriptor, None);
      assert_eq!(isbns[1].text, isbn2);
      assert_eq!(isbns[1].descriptor, None);
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
  let parsers = IsbnParser::compile().unwrap();

  let scan = parsers.scan(src);
  assert!(scan.is_some());
  let (scan, end) = scan.unwrap();
  assert_eq!(scan.text, isbn1);
  assert_eq!(scan.descriptor, None);
  assert_eq!(end, isbn1.len());

  let res = parsers.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 2);
      assert_eq!(isbns[0].text, isbn1);
      assert_eq!(isbns[0].descriptor, None);
      assert_eq!(isbns[1].text, isbn2);
      assert_eq!(isbns[1].descriptor, None);
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
  let parsers = IsbnParser::compile().unwrap();

  let scan = parsers.scan(src);
  assert!(scan.is_some());
  let (scan, end) = scan.unwrap();
  assert_eq!(scan.text, isbn1);
  assert_eq!(scan.descriptor, None);

  let res = parsers.parse(src);
  match res {
    ParseResult::Valid(isbns, trail) => {
      assert_eq!(isbns.len(), 2);
      assert_eq!(isbns[0].text, isbn1);
      assert_eq!(isbns[0].descriptor, None);
      assert_eq!(isbns[1].text, isbn2);
      assert!(isbns[1].descriptor.is_some());
      assert_eq!(isbns[1].descriptor.as_ref().unwrap(), "pbk.");
      assert_eq!(trail, "");
    },
    x => panic!("bad parse: {:?}", x)
  }
}
