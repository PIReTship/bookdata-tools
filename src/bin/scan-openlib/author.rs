//! Process author records.
use serde::Deserialize;

use bookdata::prelude::*;
use bookdata::parquet::*;

use super::common::*;

#[derive(Deserialize)]
pub struct OLAuthor {
  #[serde(default)]
  name: Option<String>,
  #[serde(default)]
  personal_name: Option<String>,
  #[serde(default)]
  alternate_names: Vec<String>
}

#[derive(TableRow)]
struct AuthorRec {
  id: u32,
  key: String,
  name: Option<String>
}

#[derive(TableRow)]
struct NameRec {
  id: u32,
  source: u8,
  name: String
}

pub struct Processor {
  last_id: u32,
  rec_writer: TableWriter<AuthorRec>,
  name_writer: TableWriter<NameRec>
}

impl OLProcessor<OLAuthor> for Processor {
  fn new() -> Result<Processor> {
    Ok(Processor {
      last_id: 0,
      rec_writer: TableWriter::open("authors.parquet")?,
      name_writer: TableWriter::open("author-names.parquet")?
    })
  }

  fn process_row(&mut self, row: Row<OLAuthor>) -> Result<()> {
    self.last_id += 1;
    let id = self.last_id;

    self.rec_writer.write_object(AuthorRec {
      id, key: row.key, name: row.record.name.clone()
    })?;

    if let Some(n) = row.record.name {
      self.name_writer.write_object(NameRec {
        id, source: b'n', name: n
      })?;
    }

    if let Some(n) = row.record.personal_name {
      self.name_writer.write_object(NameRec {
        id, source: b'p', name: n
      })?;
    }

    for n in row.record.alternate_names {
      self.name_writer.write_object(NameRec {
        id, source: b'a', name: n
      })?;
    }

    Ok(())
  }

  fn finish(self) -> Result<()> {
    self.rec_writer.finish()?;
    self.name_writer.finish()?;
    Ok(())
  }
}
