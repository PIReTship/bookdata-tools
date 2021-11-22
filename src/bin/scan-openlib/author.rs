//! Process author records.
use serde::Deserialize;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::cleaning::strings::norm_unicode;
use bookdata::schemas::openlib::author::*;

use super::common::*;

pub struct Processor {
  last_id: u32,
  rec_writer: TableWriter<AuthorRec>,
  name_writer: TableWriter<AuthorNameRec>
}

impl OLProcessor<OLAuthorSource> for Processor {
  fn new() -> Result<Processor> {
    Ok(Processor {
      last_id: 0,
      rec_writer: TableWriter::open("authors.parquet")?,
      name_writer: TableWriter::open("author-names.parquet")?
    })
  }

  fn process_row(&mut self, row: Row<OLAuthorSource>) -> Result<()> {
    self.last_id += 1;
    let id = self.last_id;

    self.rec_writer.write_object(AuthorRec {
      id, key: row.key, name: row.record.name.as_ref().map(|s| norm_unicode(s).into_owned())
    })?;

    for name in row.record.names(id) {
      self.name_writer.write_object(name)?;
    }

    Ok(())
  }

  fn finish(self) -> Result<()> {
    self.rec_writer.finish()?;
    self.name_writer.finish()?;
    Ok(())
  }
}
