//! Process and store MARC data.
//!
//! This module provides support for parsing MARC data from XML (in both
//! Library of Congress and VIAF formats), and for storing MARC data in
//! Parquet files as a flat table of MARC fields.
pub mod record;
pub mod parse;
pub mod flat_fields;
pub mod book_fields;

pub use record::MARCRecord;
