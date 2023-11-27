use std::convert::TryInto;
use std::io::BufRead;
use std::mem::replace;
use std::str;
use std::thread::{spawn, JoinHandle};

use crossbeam::channel::bounded;
use log::*;

use anyhow::{anyhow, Result};
use quick_xml::events::attributes::Attributes;
use quick_xml::events::Event;
use quick_xml::Reader;

use crate::io::ObjectWriter;
use crate::tsv::split_first;
use crate::util::StringAccumulator;

use super::record::*;

const CHUNK_LINES: usize = 1000;

#[derive(Debug, Default)]
struct Codes {
    tag: i16,
    ind1: Code,
    ind2: Code,
}

impl From<Codes> for Field {
    fn from(c: Codes) -> Field {
        Field {
            tag: c.tag,
            ind1: c.ind1,
            ind2: c.ind2,
            subfields: Vec::new(),
        }
    }
}

/// Read MARC records from XML.
pub fn scan_records<R, W>(reader: R, output: &mut W) -> Result<usize>
where
    R: BufRead,
    W: ObjectWriter<MARCRecord>,
{
    let mut reader = Reader::from_reader(reader);
    let mut nrecs = 0;
    let mut buffer = Vec::with_capacity(4096);
    loop {
        match reader.read_event_into(&mut buffer)? {
            Event::Start(ref e) => {
                let name = e.local_name();
                match name.into_inner() {
                    b"record" => {
                        let rec = read_record(&mut reader)?;
                        output.write_object(rec)?;
                        nrecs += 1;
                    }
                    _ => (),
                }
            }
            Event::Eof => return Ok(nrecs),
            _ => (),
        }
    }
}

/// Read MARC records from delimited XML.
///
/// This reader parses the XML in parallel, since XML parsing is typically
/// the bottleneck for MARC scanning.
pub fn scan_records_delim<R, W>(reader: R, output: &mut W) -> Result<usize>
where
    R: BufRead + Send + 'static,
    W: ObjectWriter<MARCRecord>,
{
    let lines = reader.lines();

    // receivers & senders for chunks of lines
    let (chunk_tx, chunk_rx) = bounded(100);

    // receivers and senders for chunks of parsed records
    let (parsed_tx, parsed_rx) = bounded(100);

    // background thread getting lines
    info!("spawning reader thread");
    let bg_read: JoinHandle<Result<usize>> = spawn(move || {
        let mut accum = Vec::with_capacity(CHUNK_LINES);
        let mut nlines = 0usize;
        for line in lines {
            let line = line?;
            let (_id, payload) = split_first(&line).ok_or_else(|| anyhow!("invalid line"))?;
            nlines += 1;
            accum.push(payload.to_owned());
            if accum.len() >= CHUNK_LINES {
                let chunk = replace(&mut accum, Vec::with_capacity(CHUNK_LINES));
                chunk_tx.send(chunk).expect("channel send failure");
            }
        }
        if accum.len() > 0 {
            chunk_tx.send(accum).expect("channel send failure");
        }
        Ok(nlines)
    });

    for i in 0..4 {
        info!("spawning parser thread {}", i + 1);
        let rx = chunk_rx.clone();
        let tx = parsed_tx.clone();
        spawn(move || {
            for chunk in rx {
                let mut records = Vec::with_capacity(chunk.len());
                for line in chunk {
                    let res = parse_record(&line);
                    records.push(res);
                }
                tx.send(records).expect("background send failed");
            }
        });
    }

    let mut nrecs = 0;
    loop {
        match parsed_rx.recv() {
            Ok(records) => {
                for rec in records {
                    let rec = rec?;
                    output.write_object(rec)?;
                    nrecs += 1;
                }
            }
            Err(_) => return Ok(nrecs),
        }
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
    let mut content = StringAccumulator::new();
    let mut record = MARCRecord {
        leader: String::new(),
        control: Vec::new(),
        fields: Vec::new(),
    };
    let mut field = Field::default();
    let mut tag = 0;
    let mut sf_code = Code::default();
    loop {
        match rdr.read_event_into(&mut buf)? {
            Event::Start(ref e) => {
                let name = e.local_name();
                match name.into_inner() {
                    b"record" => (),
                    b"leader" => {
                        content.activate();
                    }
                    b"controlfield" => {
                        tag = read_tag_attr(e.attributes())?;
                        content.activate();
                    }
                    b"datafield" => {
                        let codes = read_code_attrs(e.attributes())?;
                        field = codes.into();
                    }
                    b"subfield" => {
                        sf_code = read_sf_code_attr(e.attributes())?;
                        content.activate();
                    }
                    _ => (),
                }
            }
            Event::End(ref e) => {
                let name = e.local_name();
                match name.into_inner() {
                    b"leader" => {
                        record.leader = content.finish().to_owned();
                    }
                    b"controlfield" => record.control.push(ControlField {
                        tag: tag.try_into()?,
                        content: content.finish().to_owned(),
                    }),
                    b"subfield" => field.subfields.push(Subfield {
                        code: sf_code,
                        content: content.finish().to_owned(),
                    }),
                    b"datafield" => {
                        record.fields.push(field);
                        field = Field::default();
                    }
                    b"record" => return Ok(record),
                    _ => (),
                }
            }
            Event::Text(e) => {
                let t = e.unescape()?;
                content.add_slice(t);
            }
            Event::Eof => break,
            _ => (),
        }
    }
    Err(anyhow!("could not parse record"))
}

/// Read the tag attribute from a tag.
fn read_tag_attr(attrs: Attributes<'_>) -> Result<i16> {
    for ar in attrs {
        let a = ar?;
        if a.key.into_inner() == b"tag" {
            let tag = a.unescape_value()?;
            return Ok(tag.parse()?);
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
        let v = a.unescape_value()?;
        match a.key.into_inner() {
            b"tag" => tag = v.parse()?,
            b"ind1" => ind1 = v.as_bytes()[0].into(),
            b"ind2" => ind2 = v.as_bytes()[0].into(),
            _ => (),
        }
    }

    if tag == 0 {
        Err(anyhow!("no tag attribute found"))
    } else {
        Ok(Codes { tag, ind1, ind2 })
    }
}

/// Read the subfield code attriute from a tag
fn read_sf_code_attr(attrs: Attributes<'_>) -> Result<Code> {
    for ar in attrs {
        let a = ar?;
        if a.key.into_inner() == b"code" {
            let code = a.unescape_value()?;
            return Ok(code.as_bytes()[0].into());
        }
    }

    Err(anyhow!("no code found"))
}
