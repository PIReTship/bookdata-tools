//! MARC record representation.
//!
//! The code in this module supports records in the MARC format. It can be
//! used for processing both [bibliographic][] and [name authority][] records.
//!
//! [bibliographic]: https://www.loc.gov/marc/bibliographic/
//! [name authority]: https://www.loc.gov/marc/authority/
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

// use crate::arrow::types::ArrowTypeWrapper;

/// An indicator or subfield code.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
#[serde(transparent)]
pub struct Code {
    value: u8,
}

/// Error parsing a code
#[derive(Error, Debug)]
pub enum ParseCodeError {
    /// Error parsing a character
    #[error("failed to parse character: {0}")]
    CharError(#[from] std::char::ParseCharError),
    /// Error converting to valid bounds
    #[error("character not an ASCII character: {0}")]
    IntError(#[from] std::num::TryFromIntError),
}

/// A MARC record.
#[derive(Debug, Clone)]
pub struct MARCRecord {
    pub leader: String,
    pub control: Vec<ControlField>,
    pub fields: Vec<Field>,
}

/// A control field (00X) in a MARC record.
#[derive(Debug, Clone, Default)]
pub struct ControlField {
    pub tag: i8,
    pub content: String,
}

/// A field in a MARC record.
#[derive(Debug, Clone, Default)]
pub struct Field {
    pub tag: i16,
    pub ind1: Code,
    pub ind2: Code,
    pub subfields: Vec<Subfield>,
}

/// A subfield in a MARC record.
#[derive(Debug, Clone)]
pub struct Subfield {
    pub code: Code,
    pub content: String,
}

impl MARCRecord {
    /// Get the MARC control number.
    pub fn marc_control<'a>(&'a self) -> Option<&'a str> {
        for cf in &self.control {
            if cf.tag == 1 {
                return Some(cf.content.trim());
            }
        }

        None
    }

    /// Get the LCCN.
    pub fn lccn<'a>(&'a self) -> Option<&'a str> {
        for df in &self.fields {
            if df.tag == 10 {
                for sf in &df.subfields {
                    if sf.code == 'a' {
                        return Some(sf.content.trim());
                    }
                }
            }
        }

        None
    }

    /// Get the record status.
    pub fn rec_status(&self) -> Option<u8> {
        if self.leader.len() > 5 {
            Some(self.leader.as_bytes()[5])
        } else {
            None
        }
    }

    /// Get the record type.
    pub fn rec_type(&self) -> Option<u8> {
        if self.leader.len() > 6 {
            Some(self.leader.as_bytes()[6])
        } else {
            None
        }
    }

    /// Get the record bibliographic level.
    pub fn rec_bib_level(&self) -> Option<u8> {
        if self.leader.len() > 7 {
            Some(self.leader.as_bytes()[7])
        } else {
            None
        }
    }

    /// Get the government publication code
    pub fn gov_pub_code(&self) -> Option<u8> {
        for cf in &self.control {
            if cf.tag == 8 {
                if cf.content.len() > 28 {
                    return Some(cf.content.as_bytes()[28]);
                } else {
                    return None;
                }
            }
        }

        None
    }

    /// Query whether this record is a book.
    ///
    /// A record is a book if it meets the following:
    /// - MARC type a or t
    /// - Not a government document
    pub fn is_book(&self) -> bool {
        let ty = self.rec_type().unwrap_or_default();
        if ty == b'a' || ty == b't' {
            match self.gov_pub_code() {
                None | Some(b' ') | Some(b'|') => true,
                _ => false, // government document
            }
        } else {
            false
        }
    }
}

impl From<u8> for Code {
    #[inline]
    fn from(value: u8) -> Code {
        Code { value }
    }
}

impl From<char> for Code {
    #[inline]
    fn from(mut value: char) -> Code {
        assert!(value.is_ascii(), "value must be ASCII");
        // unify blanks
        if value == '|' {
            value = ' ';
        }
        Code { value: value as u8 }
    }
}

impl From<&Code> for char {
    #[inline]
    fn from(c: &Code) -> char {
        c.value as char
    }
}

impl From<Code> for char {
    #[inline]
    fn from(c: Code) -> char {
        c.value as char
    }
}

impl From<&Code> for u8 {
    #[inline]
    fn from(c: &Code) -> u8 {
        c.value
    }
}

impl From<Code> for u8 {
    #[inline]
    fn from(c: Code) -> u8 {
        c.value
    }
}

impl PartialEq<char> for Code {
    #[inline]
    fn eq(&self, other: &char) -> bool {
        let c: char = self.into();
        c == *other
    }
}

impl FromStr for Code {
    type Err = ParseCodeError;

    fn from_str(s: &str) -> Result<Code, Self::Err> {
        let c = char::from_str(s)?;
        let v: u8 = u8::try_from(c as u32)?;
        Ok(Code { value: v })
    }
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.value == 0 {
            f.write_str("∅")
        } else {
            (self.value as char).fmt(f)
        }
    }
}
