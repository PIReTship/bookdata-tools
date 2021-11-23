//! OpenLibrary author schemas.
use crate::cleaning::strings::norm_unicode;
use crate::prelude::*;
use crate::arrow::*;

use super::source::Row;
pub use super::source::OLAuthorSource;

/// An author record in the extracted Parquet.
#[derive(TableRow)]
pub struct AuthorRec {
  pub id: u32,
  pub key: String,
  pub name: Option<String>
}

/// An author-name record in the extracted Parquet.
#[derive(TableRow)]
pub struct AuthorNameRec {
  pub id: u32,
  pub source: u8,
  pub name: String
}

/// Get a list of author name records for an author.
pub fn author_name_records(src: &OLAuthorSource, id: u32) -> Vec<AuthorNameRec> {
  let mut names = Vec::new();

  if let Some(n) = &src.name {
    names.push(AuthorNameRec {
      id, source: b'n', name: norm_unicode(&n).into_owned()
    });
  }

  if let Some(n) = &src.personal_name {
    names.push(AuthorNameRec {
      id, source: b'p', name: norm_unicode(&n).into_owned()
    });
  }

  for n in &src.alternate_names {
    names.push(AuthorNameRec {
      id, source: b'a', name: norm_unicode(&n).into_owned()
    });
  }

  names
}

/// Process author records into Parquet.
pub struct AuthorProcessor {
  last_id: u32,
  rec_writer: TableWriter<AuthorRec>,
  name_writer: TableWriter<AuthorNameRec>
}

impl AuthorProcessor {
  pub fn new() -> Result<AuthorProcessor> {
    Ok(AuthorProcessor {
      last_id: 0,
      rec_writer: TableWriter::open("authors.parquet")?,
      name_writer: TableWriter::open("author-names.parquet")?
    })
  }
}

impl ObjectWriter<Row<OLAuthorSource>> for AuthorProcessor {
  fn write_object(&mut self, row: Row<OLAuthorSource>) -> Result<()> {
    self.last_id += 1;
    let id = self.last_id;

    self.rec_writer.write_object(AuthorRec {
      id, key: row.key, name: row.record.name.as_ref().map(|s| norm_unicode(s).into_owned())
    })?;

    for name in author_name_records(&row.record, id) {
      self.name_writer.write_object(name)?;
    }

    Ok(())
  }

  fn finish(self) -> Result<usize> {
    self.rec_writer.finish()?;
    self.name_writer.finish()?;
    Ok(self.last_id as usize)
  }
}
