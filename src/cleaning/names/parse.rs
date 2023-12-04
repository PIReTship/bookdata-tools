//! PEG parser for name variants.

use super::types::*;

peg::parser! {
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
        = last:$([^',']*) "," space()* rest:$(([_] !ending())* [^',']?) {
            if rest.trim().is_empty() {
                NameFmt::Single(last.trim().to_owned())
            } else {
                NameFmt::TwoPart(last.trim().to_owned(), rest.trim().to_owned())
            }
        }

        rule single_name() -> NameFmt
        = name:$(([_] !ending())* [^',' | '.']?) { NameFmt::Single(name.trim().to_owned()) }

        rule name() -> NameFmt = ("!!!" ['a'..='z' | 'A'..='Z']+ "!!!" space()*)? n:(cs_name() / single_name()) { n }

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
