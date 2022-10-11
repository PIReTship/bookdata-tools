use std::io::{BufRead, Lines};
use std::str;
use std::convert::TryInto;
use std::sync::mpsc::sync_channel;
use std::thread::spawn;

use rayon::prelude::*;
use quick_xml::Reader;
use quick_xml::events::Event;
use quick_xml::events::attributes::Attributes;
use fallible_iterator::FallibleIterator;
use anyhow::{Result, anyhow, Error};

use crate::util::DataAccumulator;
use crate::tsv::split_first;
use crate::util::iteration::chunk_owned;

use super::record::*;

#[derive(Debug, Default)]
struct Codes {
  tag: i16,
  ind1: Code,
  ind2: Code
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
  #[allow(dead_code)]
  DL(Lines<B>),
  /// Iterable of parsed records
  PRI(Box<dyn Iterator<Item=Result<MARCRecord,Error>>>),
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
      Src::DL(lines) => next_line(lines),
      Src::PRI(iter) => iter.next().transpose(),
    }
  }
}

fn next_qxr<B: BufRead>(reader: &mut Reader<B>, buf: &mut Vec<u8>) -> Result<Option<MARCRecord>> {
  loop {
    match reader.read_event(buf)? {
      Event::Start(ref e) => {
        let name = e.local_name();
        match name {
          b"record" => {
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
///
/// This reader uses Rayon to parse the XML in parallel, since XML parsing is typically
/// the bottleneck for MARC scanning.
pub fn read_records_delim<B: BufRead + Send + 'static>(reader: B) -> Records<B> {
  let lines = reader.lines();
  // chunk lines (to decrease threading overhead)
  let chunks = chunk_owned(lines, 5000);
  // parse batches of lines in parallel
  let chunks = chunks.par_bridge();
  let mapped = chunks.map(|batch| {
    let res: Vec<_> = batch.into_iter().map(|element| {
      match element {
        Ok(line) => parse_record(&line),
        Err(e) => Err(e.into())
      }
    }).collect();
    res
  });
  // now we need to route batches in the background, so we don't block the caller
  let (send, recv) = sync_channel(100);
  let _jh = spawn(move || {
    mapped.for_each_with(send, |send, batch| {
      send.send(batch).expect("channel error");
    });
  });

  let flat = recv.into_iter().flatten();

  Records {
    buffer: Vec::new(),
    source: Src::PRI(Box::new(flat))
  }
}

/// Parse a single MARC record from an XML string.
pub fn parse_record<S: AsRef<str>>(xml: S) -> Result<MARCRecord> {
  let mut parse = Reader::from_str(xml.as_ref());
  read_record(&mut parse)
}

/// Read a single MARC record from an XML reader.
#[inline(never)] // make profiling a little easier, this fn isn't worth inlining
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
  let mut sf_code = Code::default();
  loop {
    match rdr.read_event(&mut buf)? {
      Event::Start(ref e) => {
        let name = e.local_name();
        match name {
          b"record" => (),
          b"leader" => {
            content.activate();
          },
          b"controlfield" => {
            tag = read_tag_attr(e.attributes())?;
            content.activate();
          },
          b"datafield" => {
            let codes = read_code_attrs(e.attributes())?;
            field = codes.into();
          },
          b"subfield" => {
            sf_code = read_sf_code_attr(e.attributes())?;
            content.activate();
          }
          _ => ()
        }
      },
      Event::End(ref e) => {
        let name = e.local_name();
        match name {
          b"leader" => {
            record.leader = content.finish_string()?;
          }
          b"controlfield" => {
            record.control.push(ControlField {
              tag: tag.try_into()?,
              content: content.finish_string()?
            })
          }
          b"subfield" => {
            field.subfields.push(Subfield {
              code: sf_code,
              content: content.finish_string()?
            })
          },
          b"datafield" => {
            record.fields.push(field);
            field = Field::default();
          },
          b"record" => {
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
  let mut ind1 = Code::default();
  let mut ind2 = Code::default();

  for ar in attrs {
    let a = ar?;
    let v = a.unescaped_value()?;
    match a.key {
      b"tag" => tag = str::from_utf8(&v)?.parse()?,
      b"ind1" => ind1 = v[0].into(),
      b"ind2" => ind2 = v[0].into(),
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
fn read_sf_code_attr(attrs: Attributes<'_>) -> Result<Code> {
  for ar in attrs {
    let a = ar?;
    if a.key == b"code" {
      let code = a.unescaped_value()?;
      return Ok(code[0].into())
    }
  }

  Err(anyhow!("no code found"))
}
