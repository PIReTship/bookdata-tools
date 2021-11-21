use serde::Serialize;

use crate::prelude::*;
use crate::arrow::*;
use crate::cleaning::isbns::{ParserDefs, ParseResult};
use crate::marc::MARCRecord;
use crate::marc::flat_fields::FieldOutput;

use crate as bookdata;

#[derive(TableRow, Debug)]
struct BookIds {
  rec_id: u32,
  marc_cn: String,
  lccn: Option<String>,
  status: u8,
  rec_type: u8,
  bib_level: u8
}

#[derive(Serialize, TableRow, Debug)]
struct ISBNrec {
  rec_id: u32,
  isbn: String,
  tag: Option<String>
}

pub struct BookOutput {
  n_books: u32,
  parser: ParserDefs,
  fields: FieldOutput,
  ids: TableWriter<BookIds>,
  isbns: TableWriter<ISBNrec>
}

impl BookOutput {
  pub fn open(prefix: &str) -> Result<BookOutput> {
    let ffn = format!("{}-fields.parquet", prefix);
    info!("writing book fields to {}", ffn);
    let fields = TableWriter::open(ffn)?;
    let fields = FieldOutput::new(fields);

    let idfn = format!("{}-ids.parquet", prefix);
    info!("writing book IDs to {}", idfn);
    let ids = TableWriter::open(idfn)?;

    let isbnfn = format!("{}-isbns.parquet", prefix);
    info!("writing book IDs to {}", isbnfn);
    let isbns = TableWriter::open(isbnfn)?;

    Ok(BookOutput {
      n_books: 0,
      parser: ParserDefs::new(),
      fields, ids, isbns
    })
  }
}

impl ObjectWriter<MARCRecord> for BookOutput {
  fn write_object(&mut self, record: MARCRecord) -> Result<()> {
    if !record.is_book() {
      return Ok(())
    }
    self.n_books += 1;
    let rec_id = self.n_books;

    // scan for ISBNs
    for df in &record.fields {
      if df.tag == 20 {
        for sf in &df.subfields {
          if sf.code == 'a' {
            match self.parser.parse(&sf.content) {
              ParseResult::Valid(isbns, _) => {
                for isbn in isbns {
                  if isbn.tags.len() > 0 {
                    for tag in isbn.tags {
                      self.isbns.write_object(ISBNrec {
                        rec_id, isbn: isbn.text.clone(), tag: Some(tag)
                      })?;
                    }
                  } else {
                    self.isbns.write_object(ISBNrec {
                      rec_id, isbn: isbn.text, tag: None
                    })?;
                  }
                }
              },
              ParseResult::Ignored(_) => (),
              ParseResult::Unmatched(s) => {
                warn!("unmatched ISBN text {}", s)
              }
            }
          }
        }
      }
    }

    // emit book IDs
    let ids = BookIds {
      rec_id,
      marc_cn: record.marc_control().ok_or_else(|| {
        anyhow!("no MARC control number")
      })?.to_owned(),
      lccn: record.lccn().map(|s| s.to_owned()),
      status: record.rec_status().unwrap_or(0),
      rec_type: record.rec_type().unwrap_or(0),
      bib_level: record.rec_bib_level().unwrap_or(0)
    };
    self.ids.write_object(ids)?;

    self.fields.write_object(record)?;
    Ok(())
  }

  fn finish(self) -> Result<usize> {
    self.fields.finish()?;
    self.ids.finish()?;
    self.isbns.finish()?;
    Ok(self.n_books as usize)
  }
}
