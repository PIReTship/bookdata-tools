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

peg::parser!{
  grammar name_parser() for str {
    rule space() = quiet!{[' ' | '\n' | '\r' | '\t']}
    rule digit() = quiet!{['0'..='9']}

    rule trailing_junk() = [',' | '.'] space()* ![_]

    rule year_range() -> String
      = range:$(digit()+ "-" digit()*) { range.to_owned() }

    rule year_tag() -> String
      = (space()* ",")? space()* "("? y:year_range() ")"? {
        y
      }

    rule ending() -> Option<String>
      = trailing_junk() { None }
      / y:year_tag() { Some(y) }

    rule cs_name() -> NameFmt
      = last:$([^',']*) "," space()* rest:$(([_] !ending())+ [^',']?) {
        NameFmt::TwoPart(last.trim().to_owned(), rest.trim().to_owned())
      }

    rule single_name() -> NameFmt
      = name:$(([_] !ending())* [^',' | '.']?) { NameFmt::Single(name.trim().to_owned()) }

    rule name() -> NameFmt = ("!!!" ['a'..='z' | 'A'..='Z']+ "!!!")? n:(cs_name() / single_name()) { n }

    #[no_eof]
    pub rule name_entry() -> NameEntry
      = year:year_tag() { NameEntry { name:NameFmt::Empty, year: Some(year) } }
      / name:name() year:ending()? { NameEntry { name, year: year.flatten() } }
  }
}

pub fn parse_name_entry(name: &str) -> Result<NameEntry, NameError> {
  let res = name_parser::name_entry(name)?;
  Ok(res)
}
