use quick_xml::Reader;
use quick_xml::events::Event;

/// A MARC record.
#[derive(Debug)]
pub struct MARCRecord {
  leader: String,
  control: Vec<ControlField>,
  fields: Vec<Field>
}

/// A control field (00X) in a MARC record.
#[derive(Debug)]
pub struct ControlField {
  tag: i8,
  content: String
}

/// A field in a MARC record.
#[derive(Debug)]
pub struct Field {
  tag: i16,
  ind1: u8,
  ind2: u8,
  subfields: Vec<Subfield>
}

/// A subfield in a MARC record.
#[derive(Debug)]
pub struct Subfield {
  code: u8,
  content: String
}


fn process_records<B: BufRead>(rdr: &mut Reader<B>, writer: &mut TableWriter<Record>, start: u32) -> Result<u32> {
  let mut buf = Vec::new();
  let mut content = Vec::with_capacity(100);
  let mut record = Record::default();
  record.rec_id = start;
  loop {
    match rdr.read_event(&mut buf)? {
      Event::Start(ref e) => {
        let name = str::from_utf8(e.local_name())?;
        match name {
          "record" => {
            record.rec_id += 1;
            record.fld_no = 0;
          },
          "leader" => {
            record.tag = -1;
            content.clear();
          },
          "controlfield" => {
            record.fld_no += 1;
            let mut ntags = 0;
            for ar in e.attributes() {
              let a = ar?;
              if a.key == b"tag" {
                let tag = a.unescaped_value()?;
                record.tag = str::from_utf8(&tag)?.parse()?;
                ntags += 1;
              }
            }
            assert!(ntags == 1, "no tag found for control field");
            content.clear();
          },
          "datafield" => {
            record.fld_no += 1;
            for ar in e.attributes() {
              let a = ar?;
              let v = a.unescaped_value()?;
              match a.key {
                b"tag" => record.tag = str::from_utf8(&v)?.parse()?,
                b"ind1" => record.ind1 = v[0],
                b"ind2" => record.ind2 = v[0],
                _ => ()
              }
            }
          },
          "subfield" => {
            let mut natts = 0;
            for ar in e.attributes() {
              let a = ar?;
              if a.key == b"code" {
                let code = a.unescaped_value()?;
                record.sf_code = code[0];
                natts += 1;
              }
            }
            assert!(natts >= 1, "no code found for subfield");
            assert!(natts <= 1, "too many codes found for subfield");
            content.clear();
          }
          _ => ()
        }
      },
      Event::End(ref e) => {
        let name = str::from_utf8(e.local_name())?;
        match name {
          "leader" | "controlfield" | "subfield" => {
            record.contents = String::from_utf8(content.clone())?;
            writer.write(&record)?;
          },
          "datafield" => {
            record.ind1 = 0;
            record.ind2 = 0;
            record.sf_code = 0;
            record.contents = String::new();
          },
          _ => ()
        }
      },
      Event::Text(e) => {
        let t = e.unescaped()?;
        content.extend_from_slice(&t);
      },
      Event::Eof => break,
      _ => ()
    }
  }
  Ok(record.rec_id - start)
}


fn read_record<B: BufRead>(rdr: &mut Reader<B>) -> Result<MARCRecord> {
  let mut buf = Vec::new();
  let mut content = Vec::with_capacity(100);
  let mut record = Record::default();
  record.rec_id = start;
  loop {
    match rdr.read_event(&mut buf)? {
      Event::Start(ref e) => {
        let name = str::from_utf8(e.local_name())?;
        match name {
          "record" => {
            record.rec_id += 1;
            record.fld_no = 0;
          },
          "leader" => {
            record.tag = -1;
            content.clear();
          },
          "controlfield" => {
            record.fld_no += 1;
            let mut ntags = 0;
            for ar in e.attributes() {
              let a = ar?;
              if a.key == b"tag" {
                let tag = a.unescaped_value()?;
                record.tag = str::from_utf8(&tag)?.parse()?;
                ntags += 1;
              }
            }
            assert!(ntags == 1, "no tag found for control field");
            content.clear();
          },
          "datafield" => {
            record.fld_no += 1;
            for ar in e.attributes() {
              let a = ar?;
              let v = a.unescaped_value()?;
              match a.key {
                b"tag" => record.tag = str::from_utf8(&v)?.parse()?,
                b"ind1" => record.ind1 = v[0],
                b"ind2" => record.ind2 = v[0],
                _ => ()
              }
            }
          },
          "subfield" => {
            let mut natts = 0;
            for ar in e.attributes() {
              let a = ar?;
              if a.key == b"code" {
                let code = a.unescaped_value()?;
                record.sf_code = code[0];
                natts += 1;
              }
            }
            assert!(natts >= 1, "no code found for subfield");
            assert!(natts <= 1, "too many codes found for subfield");
            content.clear();
          }
          _ => ()
        }
      },
      Event::End(ref e) => {
        let name = str::from_utf8(e.local_name())?;
        match name {
          "leader" | "controlfield" | "subfield" => {
            record.contents = String::from_utf8(content.clone())?;
            writer.write(&record)?;
          },
          "datafield" => {
            record.ind1 = 0;
            record.ind2 = 0;
            record.sf_code = 0;
            record.contents = String::new();
          },
          _ => ()
        }
      },
      Event::Text(e) => {
        let t = e.unescaped()?;
        content.extend_from_slice(&t);
      },
      Event::Eof => break,
      _ => ()
    }
  }
  Ok(record.rec_id - start)
}
