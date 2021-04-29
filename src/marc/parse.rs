use std::io::{BufRead, Lines};
use std::str;
use std::convert::TryInto;

use quick_xml::Reader;
use quick_xml::events::Event;
use quick_xml::events::attributes::Attributes;
use fallible_iterator::FallibleIterator;
use anyhow::{Result, anyhow};

use crate::util::DataAccumulator;
use crate::tsv::split_first;

use super::record::*;

#[derive(Debug, Default)]
struct Codes {
  tag: i16,
  ind1: u8,
  ind2: u8
}

impl From<Codes> for Field {
  fn from(c: Codes) -> Field {
    Field {
      tag: c.tag,
      ind1: c.ind1,
      ind2: c.ind2,
      subfields: Vec::new()
    }
  }
}

/// Sources of MARC data.
enum Src<B: BufRead> {
  /// QuickXML Reader
  QXR(Reader<B>),
  /// Delimited lines
  DL(Lines<B>)
}

/// Iterator of MARC records.
pub struct Records<B: BufRead> {
  buffer: Vec<u8>,
  source: Src<B>
}

impl <B: BufRead> FallibleIterator for Records<B> {
  type Error = anyhow::Error;
  type Item = MARCRecord;

  fn next(&mut self) -> Result<Option<MARCRecord>> {
    match &mut self.source {
      Src::QXR(reader) => next_qxr(reader, &mut self.buffer),
      Src::DL(lines) => next_line(lines)
    }
  }
}

fn next_qxr<B: BufRead>(reader: &mut Reader<B>, buf: &mut Vec<u8>) -> Result<Option<MARCRecord>> {
  loop {
    match reader.read_event(buf)? {
      Event::Start(ref e) => {
        let name = str::from_utf8(e.local_name())?;
        match name {
          "record" => {
            return Ok(Some(read_record(reader)?))
          },
          _ => ()
        }
      },
      Event::Eof => return Ok(None),
      _ => ()
    }
  }
}

fn next_line<B: BufRead>(lines: &mut Lines<B>) -> Result<Option<MARCRecord>> {
  match lines.next() {
    None => Ok(None),
    Some(line) => {
      let lstr = line?;
      let (_id, xml) = split_first(&lstr).ok_or(anyhow!("invalid line"))?;
      let rec = parse_record(&xml)?;
      Ok(Some(rec))
    }
  }
}

/// Read MARC records from XML.
pub fn read_records<B: BufRead>(reader: B) -> Records<B> {
  let reader = Reader::from_reader(reader);
  Records {
    buffer: Vec::with_capacity(1024),
    source: Src::QXR(reader)
  }
}

/// Read MARC records from delimited XML.
pub fn read_records_delim<B: BufRead>(reader: B) -> Records<B> {
  Records {
    buffer: Vec::with_capacity(1024),
    source: Src::DL(reader.lines())
  }
}

/// Read a single MARC record from XML.
pub fn read_single_record<B: BufRead>(reader: &mut Reader<B>) -> Result<MARCRecord> {
  read_record(reader)
}

/// Parse a single MARC record from an XML string.
pub fn parse_record<S: AsRef<str>>(xml: S) -> Result<MARCRecord> {
  let mut parse = Reader::from_str(xml.as_ref());
  read_record(&mut parse)
}

/// Read a single MARC record from an XML reader.
fn read_record<B: BufRead>(rdr: &mut Reader<B>) -> Result<MARCRecord> {
  let mut buf = Vec::new();
  let mut content = DataAccumulator::new();
  let mut record = MARCRecord {
    leader: String::new(),
    control: Vec::new(),
    fields: Vec::new()
  };
  let mut field = Field::default();
  let mut tag = 0;
  let mut sf_code = 0;
  loop {
    match rdr.read_event(&mut buf)? {
      Event::Start(ref e) => {
        let name = str::from_utf8(e.local_name())?;
        match name {
          "record" => (),
          "leader" => {
            content.activate();
          },
          "controlfield" => {
            tag = read_tag_attr(e.attributes())?;
            content.activate();
          },
          "datafield" => {
            let codes = read_code_attrs(e.attributes())?;
            field = codes.into();
          },
          "subfield" => {
            sf_code = read_sf_code_attr(e.attributes())?;
            content.activate();
          }
          _ => ()
        }
      },
      Event::End(ref e) => {
        let name = str::from_utf8(e.local_name())?;
        match name {
          "leader" => {
            record.leader = content.finish_string()?;
          }
          "controlfield" => {
            record.control.push(ControlField {
              tag: tag.try_into()?,
              content: content.finish_string()?
            })
          }
          "subfield" => {
            field.subfields.push(Subfield {
              code: sf_code,
              content: content.finish_string()?
            })
          },
          "datafield" => {
            record.fields.push(field);
            field = Field::default();
          },
          "record" => {
            return Ok(record)
          },
          _ => ()
        }
      },
      Event::Text(e) => {
        let t = e.unescaped()?;
        content.add_slice(&t);
      },
      Event::Eof => break,
      _ => ()
    }
  }
  Err(anyhow!("could not parse record"))
}

/// Read the tag attribute from a tag.
fn read_tag_attr(attrs: Attributes<'_>) -> Result<i16> {
  for ar in attrs {
    let a = ar?;
    if a.key == b"tag" {
      let tag = a.unescaped_value()?;
      return Ok(str::from_utf8(&tag)?.parse()?);
    }
  }

  Err(anyhow!("no tag attribute found"))
}

/// Read code attributes from a tag.
fn read_code_attrs(attrs: Attributes<'_>) -> Result<Codes> {
  let mut tag = 0;
  let mut ind1 = 0;
  let mut ind2 = 0;

  for ar in attrs {
    let a = ar?;
    let v = a.unescaped_value()?;
    match a.key {
      b"tag" => tag = str::from_utf8(&v)?.parse()?,
      b"ind1" => ind1 = v[0],
      b"ind2" => ind2 = v[0],
      _ => ()
    }
  }

  if tag == 0 {
    Err(anyhow!("no tag attribute found"))
  } else {
    Ok(Codes {
      tag, ind1, ind2
    })
  }
}

/// Read the subfield code attriute from a tag
fn read_sf_code_attr(attrs: Attributes<'_>) -> Result<u8> {
  for ar in attrs {
    let a = ar?;
    if a.key == b"code" {
      let code = a.unescaped_value()?;
      return Ok(code[0])
    }
  }

  Err(anyhow!("no code found"))
}
