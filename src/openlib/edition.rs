//! OpenLibrary edition schemas.
use crate::prelude::*;
use crate::arrow::*;
use crate::ids::index::IdIndex;
use crate::cleaning::isbns::clean_asin_chars;
use crate::cleaning::isbns::clean_isbn_chars;

use super::source::Row;
pub use super::source::OLEditionRecord;

/// An edition row in the extracted Parquet.
#[derive(TableRow)]
pub struct EditionRec {
  pub id: u32,
  pub key: String,
  pub title: Option<String>,
}

/// Link between edition and work.
#[derive(TableRow)]
pub struct LinkRec {
  pub edition: u32,
  pub work: u32
}

/// Edition ISBN record.
#[derive(TableRow)]
pub struct ISBNrec {
  pub edition: u32,
  pub isbn: String
}

/// Edition author record.
#[derive(TableRow)]
pub struct EditionAuthorRec {
  pub edition: u32,
  pub pos: u16,
  pub author: u32
}

/// Process edition records into Parquet.
///
/// This must be run **after** the author and work processors.
pub struct EditionProcessor {
  last_id: u32,
  author_ids: IdIndex<String>,
  work_ids: IdIndex<String>,
  rec_writer: TableWriter<EditionRec>,
  link_writer: TableWriter<LinkRec>,
  isbn_writer: TableWriter<ISBNrec>,
  author_writer: TableWriter<EditionAuthorRec>
}

impl EditionProcessor {
  pub fn new() -> Result<EditionProcessor> {
    Ok(EditionProcessor {
      last_id: 0,
      author_ids: IdIndex::load_standard("author-ids-after-works.parquet")?,
      work_ids: IdIndex::load_standard("works.parquet")?,
      rec_writer: TableWriter::open("editions.parquet")?,
      link_writer: TableWriter::open("edition-works.parquet")?,
      isbn_writer: TableWriter::open("edition-isbns.parquet")?,
      author_writer: TableWriter::open("edition-authors.parquet")?
    })
  }

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

impl ObjectWriter<Row<OLEditionRecord>> for EditionProcessor {
  fn write_object(&mut self, row: Row<OLEditionRecord>) -> Result<()> {
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

  fn finish(self) -> Result<usize> {
    self.rec_writer.finish()?;
    self.author_writer.finish()?;
    self.link_writer.finish()?;
    self.isbn_writer.finish()?;
    self.author_ids.save_standard("all-authors.parquet")?;
    self.work_ids.save_standard("all-works.parquet")?;
    Ok(self.last_id as usize)
  }
}

impl EditionProcessor {

}
