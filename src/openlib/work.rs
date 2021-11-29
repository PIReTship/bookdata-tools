//! OpenLibrary work schemas.
use crate::prelude::*;
use crate::arrow::*;
use crate::ids::index::IdIndex;

use super::source::Row;
pub use super::source::OLWorkRecord;

/// Work row in extracted Parquet.
#[derive(TableRow)]
pub struct WorkRec {
  pub id: u32,
  pub key: String,
  pub title: Option<String>,
}

/// Work-author link in extracted Parquet.
#[derive(TableRow)]
pub struct WorkAuthorRec {
  pub id: u32,
  pub pos: u16,
  pub author: u32
}

/// Process author source records into Parquet.
///
/// This must be run **after** the author processor.
pub struct WorkProcessor {
  last_id: u32,
  author_ids: IdIndex<String>,
  rec_writer: TableWriter<WorkRec>,
  author_writer: TableWriter<WorkAuthorRec>
}

impl WorkProcessor {
  /// Create a new work processor.
  pub fn new() -> Result<WorkProcessor> {
    Ok(WorkProcessor {
      last_id: 0,
      author_ids: IdIndex::load_standard("authors.parquet")?,
      rec_writer: TableWriter::open("works.parquet")?,
      author_writer: TableWriter::open("work-authors.parquet")?
    })
  }
}

impl ObjectWriter<Row<OLWorkRecord>> for WorkProcessor {
  fn write_object(&mut self, row: Row<OLWorkRecord>) -> Result<()> {
    self.last_id += 1;
    let id = self.last_id;

    self.rec_writer.write_object(WorkRec {
      id, key: row.key.clone(), title: row.record.title.clone(),
    })?;

    for pos in 0..row.record.authors.len() {
      let akey = row.record.authors[pos].key();
      if let Some(akey) = akey {
        let aid = self.author_ids.intern(akey);
        let pos = pos as u16;
        self.author_writer.write_object(WorkAuthorRec {
          id, pos, author: aid
        })?;
      }
    }

    Ok(())
  }

  fn finish(self) -> Result<usize> {
    self.rec_writer.finish()?;
    self.author_writer.finish()?;
    self.author_ids.save_standard("author-ids-after-works.parquet")?;
    Ok(self.last_id as usize)
  }
}
