use serde::Deserialize;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::cleaning::isbns::*;
use bookdata::ids::index::IdIndex;

use crate::common::*;

#[derive(Deserialize)]
pub struct OLEditionRecord {
  #[serde(default)]
  isbn_10: Vec<String>,
  #[serde(default)]
  isbn_13: Vec<String>,
  #[serde(default)]
  asin: Vec<String>,

  #[serde(default)]
  title: Option<String>,

  #[serde(default)]
  works: Vec<Keyed>,
  #[serde(default)]
  authors: Vec<Author>,
}

#[derive(TableRow)]
struct EditionRec {
  id: u32,
  key: String,
  title: Option<String>,
}

/// Link between edition and work
#[derive(TableRow)]
struct LinkRec {
  edition: u32,
  work: u32
}

#[derive(TableRow)]
struct ISBNrec {
  edition: u32,
  isbn: String
}

#[derive(TableRow)]
struct EditionAuthorRec {
  edition: u32,
  pos: u16,
  author: u32
}

pub struct Processor {
  last_id: u32,
  author_ids: IdIndex<String>,
  work_ids: IdIndex<String>,
  rec_writer: TableWriter<EditionRec>,
  link_writer: TableWriter<LinkRec>,
  isbn_writer: TableWriter<ISBNrec>,
  author_writer: TableWriter<EditionAuthorRec>
}

impl OLProcessor<OLEditionRecord> for Processor {
  fn new() -> Result<Processor> {
    Ok(Processor {
      last_id: 0,
      author_ids: IdIndex::load_standard("author-ids-after-works.parquet")?,
      work_ids: IdIndex::load_standard("works.parquet")?,
      rec_writer: TableWriter::open("editions.parquet")?,
      link_writer: TableWriter::open("edition-works.parquet")?,
      isbn_writer: TableWriter::open("edition-isbns.parquet")?,
      author_writer: TableWriter::open("edition-authors.parquet")?
    })
  }

  fn process_row(&mut self, row: Row<OLEditionRecord>) -> Result<()> {
    self.last_id += 1;
    let id = self.last_id;

    self.rec_writer.write_object(EditionRec {
      id, key: row.key.clone(), title: row.record.title.clone(),
    })?;

    self.save_isbns(id, row.record.isbn_10, clean_isbn_chars)?;
    self.save_isbns(id, row.record.isbn_13, clean_isbn_chars)?;
    self.save_isbns(id, row.record.asin, clean_asin_chars)?;

    for work in row.record.works {
      let work = self.work_ids.intern_owned(work.key);
      self.link_writer.write_object(LinkRec {
        edition: id, work
      })?;
    }

    for pos in 0..row.record.authors.len() {
      let akey = row.record.authors[pos].key();
      if let Some(akey) = akey {
        let aid = self.author_ids.intern(akey);
        let pos = pos as u16;
        self.author_writer.write_object(EditionAuthorRec {
          edition: id, pos, author: aid
        })?;
      }
    }

    Ok(())
  }

  fn finish(self) -> Result<()> {
    self.rec_writer.finish()?;
    self.author_writer.finish()?;
    self.link_writer.finish()?;
    self.isbn_writer.finish()?;
    self.author_ids.save_standard("all-authors.parquet")?;
    self.work_ids.save_standard("all-works.parquet")?;
    Ok(())
  }
}

impl Processor {
  fn save_isbns(&mut self, edition: u32, isbns: Vec<String>, clean: fn(&str) -> String) -> Result<()> {
    for isbn in isbns {
      let isbn = clean(&isbn);
      // filter but with a reasonable threshold of error
      if isbn.len() >= 8 {
        self.isbn_writer.write_object(ISBNrec {
          edition, isbn
        })?;
      }
    }

    Ok(())
  }
}
