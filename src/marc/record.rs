//! MARC record representation.
//!
//! The code in this module supports records in the MARC format. It can be
//! used for processing both [bibliographic][] and [name authority][] records.
//!
//! [bibliographic]: https://www.loc.gov/marc/bibliographic/
//! [name authority]: https://www.loc.gov/marc/authority/

/// A MARC record.
#[derive(Debug, Clone)]
pub struct MARCRecord {
  pub leader: String,
  pub control: Vec<ControlField>,
  pub fields: Vec<Field>
}

/// A control field (00X) in a MARC record.
#[derive(Debug, Clone, Default)]
pub struct ControlField {
  pub tag: i8,
  pub content: String
}

/// A field in a MARC record.
#[derive(Debug, Clone, Default)]
pub struct Field {
  pub tag: i16,
  pub ind1: u8,
  pub ind2: u8,
  pub subfields: Vec<Subfield>
}

/// A subfield in a MARC record.
#[derive(Debug, Clone)]
pub struct Subfield {
  pub code: u8,
  pub content: String
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
          if sf.code == b'a' {
            return Some(sf.content.trim())
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
          return Some(cf.content.as_bytes()[28])
        } else {
          return None
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
        _ => false  // government document
      }
    } else {
      false
    }
  }
}
