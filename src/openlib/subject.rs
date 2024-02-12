/// Recording OpenLibrary subjects and subject-entity links.
use num_enum::{IntoPrimitive, TryFromPrimitive};
use parse_display::*;

use super::source::OLSubjects;

/// The type of a subject relationship.
#[derive(Debug, Clone, FromStr, Display, IntoPrimitive, TryFromPrimitive)]
#[display(style = "kebab-case")]
#[repr(u8)]
pub enum SubjectType {
    General = b'G',
    Person = b'P',
    Place = b'L',
    Time = b'T',
}

/// Schema for subject linking records.
#[derive(Debug, Clone)]
pub struct SubjectEntry {
    pub entity: u32,
    pub subj_type: SubjectType,
    pub subject: String,
}

impl OLSubjects {
    pub fn subject_records(self, entity: u32) -> Vec<SubjectEntry> {
        let mut records = Vec::new();
        for subject in self.subjects {
            records.push(SubjectEntry {
                entity,
                subj_type: SubjectType::General,
                subject: subject.into(),
            });
        }

        for subject in self.subject_people {
            records.push(SubjectEntry {
                entity,
                subj_type: SubjectType::Person,
                subject: subject.into(),
            });
        }

        for subject in self.subject_places {
            records.push(SubjectEntry {
                entity,
                subj_type: SubjectType::Place,
                subject: subject.into(),
            });
        }

        for subject in self.subject_times {
            records.push(SubjectEntry {
                entity,
                subj_type: SubjectType::Time,
                subject: subject.into(),
            });
        }

        records
    }
}
