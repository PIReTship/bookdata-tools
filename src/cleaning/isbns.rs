//! Code for cleaning up ISBNs.
//!
//! This module contains two families of functions:
//!
//! - The simple character-cleaning functions [clean_isbn_chars] and [clean_asin_chars].
//! - The full multi-ISBN parser [parse_isbn_string].
//!
//! When a string is a relatively well-formed ISBN (or ASIN), the character-cleaning functions
//! are fine.  Some sources, however (such as the Library of Congress) have messy ISBNs that
//! may have multiple ISBNs in one string, descriptive tags, and all manner of other messes.
//! The multi-ISBN parser exposed through [parse_isbn_string] supports cleaning these ISBN
//! strings using a PEG-based parser.
use lazy_static::lazy_static;
use regex::RegexSet;

use crate::util::unicode::NONSPACING_MARK;

/// Single ISBN parsed from a string.
#[derive(Debug, PartialEq)]
pub struct ISBN {
    pub text: String,
    pub tags: Vec<String>,
}

/// Result of parsing an ISBN string.
#[derive(Debug, PartialEq)]
pub enum ParseResult {
    /// Vaid ISBNs, possibly with additional trailing text
    Valid(Vec<ISBN>, String),
    /// An ignored string
    Ignored(String),
    /// An unparsable string
    Unmatched(String),
}

/// Regular expressions for unparsable ISBN strings to ignore.
/// This cleans up warning displays.
static IGNORES: &'static [&'static str] = &[
    r"^[$]?[[:digit:]., ]+(?:[a-zA-Z*]{1,4})?(\s+\(.*?\))?$",
    r"^[[:digit:].]+(/[[:digit:].]+)+$",
    r"^[A-Z]-[A-Z]-\d{8,}",
    r"^\s*$",
];

lazy_static! {
    static ref IGNORE_RE: RegexSet = RegexSet::new(IGNORES).unwrap();
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
        = mc:['0'..='9' | '-' | 'O'] { mc }
        / mc:[c if NONSPACING_MARK.contains(c)] { mc }

        // some ISBNs have some random junk in the middle, match allowed junk
        rule inner_junk() = ['a'..='z' | 'A'..='Z']+ / [' ' | '+']

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
        } else if b == b'O' {
            vec.push(b'0')
        }
    }
    unsafe {
        // we know it's only ascii
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
    unsafe {
        // we know it's only ascii
        String::from_utf8_unchecked(vec)
    }
}

/// Parse an ISBN string.
pub fn parse_isbn_string(s: &str) -> ParseResult {
    // let mut parser = self.create_parser(s);
    // parser.read_all()
    if let Ok((isbns, tail)) = isbn_parser::parse_isbns(s) {
        if isbns.is_empty() {
            if IGNORE_RE.is_match(s) {
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

#[test]
fn test_parse_empty() {
    let res = parse_isbn_string("");
    assert_eq!(res, ParseResult::Ignored("".to_owned()));
}

#[test]
fn test_parse_ws() {
    let res = parse_isbn_string("  ");
    assert_eq!(res, ParseResult::Ignored("  ".to_owned()));
}

#[test]
fn test_parse_isbn() {
    let isbn = "349224010X";

    let res = parse_isbn_string(isbn);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 1);
            assert_eq!(isbns[0].text, isbn);
            assert_eq!(isbns[0].tags.len(), 0);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_isbn_trail() {
    let src = "349224010X :";
    let isbn = "349224010X";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 1);
            assert_eq!(isbns[0].text, isbn);
            assert_eq!(isbns[0].tags.len(), 0);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_isbn_caron() {
    let src = "349̌224010X";
    let isbn = "349224010X";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 1);
            assert_eq!(isbns[0].text, isbn);
            assert_eq!(isbns[0].tags.len(), 0);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_hyphen_isbn() {
    let src = "978-03-2948-9391";
    let isbn = "9780329489391";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 1);
            assert_eq!(isbns[0].text, isbn);
            assert_eq!(isbns[0].tags.len(), 0);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_space_isbn() {
    let src = "978 032948-9391";
    let isbn = "9780329489391";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 1);
            assert_eq!(isbns[0].text, isbn);
            assert_eq!(isbns[0].tags.len(), 0);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_isbn_tag() {
    let src = "34922401038 (set)";
    let isbn = "34922401038";
    let tag = "set";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 1);
            assert_eq!(isbns[0].text, isbn);
            assert_eq!(isbns[0].tags, vec![tag]);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_isbn_square_tag() {
    let src = "34922401038 [set]";
    let isbn = "34922401038";
    let tag = "set";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 1);
            assert_eq!(isbns[0].text, isbn);
            assert_eq!(isbns[0].tags, vec![tag]);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_isbn_multi_tag_sep() {
    let src = "34922401038 (set : alk. paper)";
    let isbn = "34922401038";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 1);
            assert_eq!(isbns[0].text, isbn);
            assert_eq!(isbns[0].tags, vec!["set", "alk. paper"]);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_isbn_tags() {
    let src = "34922401038 (pbk.) (set)";
    let isbn = "34922401038";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 1);
            assert_eq!(isbns[0].text, isbn);
            assert_eq!(isbns[0].tags, vec!["pbk.", "set"]);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_isbn_leader() {
    let src = "a 970238408138";
    let isbn = "970238408138";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 1);
            assert_eq!(isbns[0].text, isbn);
            assert_eq!(isbns[0].tags.len(), 0);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_two_isbns_ws() {
    let src = "970238408138 30148100103";
    let isbn1 = "970238408138";
    let isbn2 = "30148100103";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 2);
            assert_eq!(isbns[0].text, isbn1);
            assert_eq!(isbns[0].tags.len(), 0);
            assert_eq!(isbns[1].text, isbn2);
            assert_eq!(isbns[1].tags.len(), 0);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_two_isbns_semi() {
    let src = "970238408138; ISBN 30148100103";
    let isbn1 = "970238408138";
    let isbn2 = "30148100103";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 2);
            assert_eq!(isbns[0].text, isbn1);
            assert_eq!(isbns[0].tags.len(), 0);
            assert_eq!(isbns[1].text, isbn2);
            assert_eq!(isbns[1].tags.len(), 0);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
fn test_parse_two_isbns_real() {
    // Real example from record 2175696
    let src = "8719359022. ISBN 8719359004 (pbk.)";
    let isbn1 = "8719359022";
    let isbn2 = "8719359004";

    let res = parse_isbn_string(src);
    match res {
        ParseResult::Valid(isbns, trail) => {
            assert_eq!(isbns.len(), 2);
            assert_eq!(isbns[0].text, isbn1);
            assert_eq!(isbns[0].tags.len(), 0);
            assert_eq!(isbns[1].text, isbn2);
            assert_eq!(isbns[1].tags, vec!["pbk."]);
            assert_eq!(trail, "");
        }
        x => panic!("bad parse: {:?}", x),
    }
}

#[test]
pub fn test_parse_isbn_junk_colon() {
    let src = "95l3512401 :";
    let isbn = "953512401";
    let isbns = parse_isbn_string(src);
    if let ParseResult::Valid(isbns, _tail) = isbns {
        assert_eq!(isbns.len(), 1);
        assert_eq!(&isbns[0].text, isbn);
    } else {
        panic!("failed to parse {}: {:?}", src, isbns);
    }
}

#[test]
pub fn test_parse_isbn_oh() {
    let src = "O882970208 (pbk.)";
    let isbn = "0882970208";

    let isbns = parse_isbn_string(src);
    if let ParseResult::Valid(isbns, _tail) = isbns {
        assert_eq!(isbns.len(), 1);
        assert_eq!(&isbns[0].text, isbn);
    } else {
        panic!("failed to parse {}: {:?}", src, isbns);
    }
}
