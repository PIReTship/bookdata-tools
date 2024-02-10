//! OpenLibrary author schemas.
use parquet_derive::ParquetRecordWriter;

use crate::arrow::*;
use crate::cleaning::names::clean_name;
use crate::cleaning::strings::norm_unicode;
use crate::prelude::*;

pub use super::source::OLAuthorSource;
use super::source::Row;

/// An author record in the extracted Parquet.
#[derive(ParquetRecordWriter)]
pub struct AuthorRec {
    pub id: i32,
    pub key: String,
    pub name: Option<String>,
}

/// An author-name record in the extracted Parquet.
#[derive(ParquetRecordWriter)]
pub struct AuthorNameRec {
    pub id: i32,
    pub source: u8,
    pub name: String,
}

/// Get a list of author name records for an author.
pub fn author_name_records(src: &OLAuthorSource, id: i32) -> Vec<AuthorNameRec> {
    let mut names = Vec::new();

    if let Some(n) = &src.name {
        names.push(AuthorNameRec {
            id,
            source: b'n',
            name: clean_name(&n),
        });
    }

    if let Some(n) = &src.personal_name {
        names.push(AuthorNameRec {
            id,
            source: b'p',
            name: clean_name(&n),
        });
    }

    for n in &src.alternate_names {
        names.push(AuthorNameRec {
            id,
            source: b'a',
            name: clean_name(&n),
        });
    }

    names
}

/// Process author records into Parquet.
pub struct AuthorProcessor {
    last_id: i32,
    rec_writer: TableWriter<AuthorRec>,
    name_writer: TableWriter<AuthorNameRec>,
}

impl AuthorProcessor {
    pub fn new() -> Result<AuthorProcessor> {
        Ok(AuthorProcessor {
            last_id: 0,
            rec_writer: TableWriter::open("authors.parquet")?,
            name_writer: TableWriter::open("author-names.parquet")?,
        })
    }
}

impl ObjectWriter<Row<OLAuthorSource>> for AuthorProcessor {
    fn write_object(&mut self, row: Row<OLAuthorSource>) -> Result<()> {
        self.last_id += 1;
        let id = self.last_id;

        self.rec_writer.write_object(AuthorRec {
            id,
            key: row.key,
            name: row
                .record
                .name
                .as_ref()
                .map(|s| norm_unicode(s).into_owned()),
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
