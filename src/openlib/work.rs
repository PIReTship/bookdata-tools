//! OpenLibrary work schemas.
use friendly::scalar;

use crate::arrow::*;
use crate::ids::index::IdIndex;
use crate::prelude::*;

pub use super::source::OLWorkRecord;
use super::source::Row;
use super::subject::{SubjectEntry, SubjectType};

/// Work row in extracted Parquet.
#[derive(Debug, Clone, TableRow)]
pub struct WorkRec {
    pub id: i32,
    pub key: String,
    pub title: Option<String>,
}

/// Work-author link in extracted Parquet.
#[derive(Debug, Clone, TableRow)]
pub struct WorkAuthorRec {
    pub id: i32,
    pub pos: i16,
    pub author: i32,
}

/// Work-subject record in extracted Parquet.
#[derive(Debug, Clone, TableRow)]
pub struct WorkSubjectRec {
    pub id: i32,
    pub subj_type: SubjectType,
    pub subject: String,
}

impl From<SubjectEntry> for WorkSubjectRec {
    fn from(value: SubjectEntry) -> Self {
        WorkSubjectRec {
            id: value.entity,
            subj_type: value.subj_type.into(),
            subject: value.subject,
        }
    }
}

/// Process author source records into Parquet.
///
/// This must be run **after** the author processor.
pub struct WorkProcessor {
    last_id: i32,
    author_ids: IdIndex<String>,
    rec_writer: TableWriter<WorkRec>,
    author_writer: TableWriter<WorkAuthorRec>,
    subject_writer: TableWriter<WorkSubjectRec>,
}

impl WorkProcessor {
    /// Create a new work processor.
    pub fn new() -> Result<WorkProcessor> {
        Ok(WorkProcessor {
            last_id: 0,
            author_ids: IdIndex::load_standard("authors.parquet")?,
            rec_writer: TableWriter::open("works.parquet")?,
            author_writer: TableWriter::open("work-authors.parquet")?,
            subject_writer: TableWriter::open("work-subjects.parquet")?,
        })
    }
}

impl ObjectWriter<Row<OLWorkRecord>> for WorkProcessor {
    fn write_object(&mut self, row: Row<OLWorkRecord>) -> Result<()> {
        self.last_id += 1;
        let id = self.last_id;

        self.rec_writer.write_object(WorkRec {
            id,
            key: row.key.clone(),
            title: row.record.title.clone(),
        })?;

        for pos in 0..row.record.authors.len() {
            let akey = row.record.authors[pos].key();
            if let Some(akey) = akey {
                let aid = self.author_ids.intern(akey)?;
                let pos = pos as i16;
                self.author_writer.write_object(WorkAuthorRec {
                    id,
                    pos,
                    author: aid,
                })?;
            }
        }

        for sr in row.record.subjects.subject_records(id) {
            self.subject_writer.write_object(sr.into())?;
        }

        Ok(())
    }

    fn finish(self) -> Result<usize> {
        let nr = self.rec_writer.finish()?;
        info!("wrote {} work records", scalar(nr));
        let na = self.author_writer.finish()?;
        info!("wrote {} work-author records", scalar(na));
        let ns = self.subject_writer.finish()?;
        info!("wrote {} work-subject records", scalar(ns));
        self.author_ids
            .save_standard("author-ids-after-works.parquet")?;
        Ok(self.last_id as usize)
    }
}
