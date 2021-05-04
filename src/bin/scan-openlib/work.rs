use serde::Deserialize;

use bookdata::prelude::*;
use bookdata::parquet::*;
use bookdata::ids::index::IdIndex;

use crate::common::*;

#[derive(Deserialize)]
pub struct OLWorkRecord {
  #[serde(default)]
  authors: Vec<Author>,
}

#[derive(TableRow)]
struct WorkRec {
  id: u32,
  key: String
}

#[derive(TableRow)]
struct WorkAuthorRec {
  id: u32,
  pos: u16,
  author: u32
}

pub struct Processor {
  last_id: u32,
  author_ids: IdIndex<String>,
  rec_writer: TableWriter<WorkRec>,
  author_writer: TableWriter<WorkAuthorRec>
}

impl OLProcessor<OLWorkRecord> for Processor {
  fn new() -> Result<Processor> {
    Ok(Processor {
      last_id: 0,
      author_ids: IdIndex::load_standard("authors.parquet")?,
      rec_writer: TableWriter::open("works.parquet")?,
      author_writer: TableWriter::open("work-authors.parquet")?
    })
  }

  fn process_row(&mut self, row: Row<OLWorkRecord>) -> Result<()> {
    self.last_id += 1;
    let id = self.last_id;

    self.rec_writer.write_object(WorkRec {
      id, key: row.key.clone()
    })?;

    for pos in 0..row.record.authors.len() {
      let akey = row.record.authors[pos].key();
      if let Some(akey) = akey {
        let aid = self.author_ids.lookup(akey);
        if let Some(aid) = aid {
          let pos = pos as u16;
          self.author_writer.write_object(WorkAuthorRec {
            id, pos, author: aid
          })?;
        } else {
          warn!("unknown author {} on work {}", akey, row.key);
        }
      }
    }

    Ok(())
  }

  fn finish(self) -> Result<()> {
    self.rec_writer.finish()?;
    self.author_writer.finish()?;
    Ok(())
  }
}
