//! GoodReads book identifier and linking support.
use std::collections::HashMap;

use anyhow::Result;
use log::*;
use serde::{Deserialize, Serialize};

use crate::{arrow::*, prelude::BDPath};

pub type BookId = i32;
pub type WorkId = i32;
pub type BookLinkMap = HashMap<BookId, BookLinkRecord>;

const GR_LINK_FILE: BDPath<'static> = BDPath::new("goodreads/gr-book-link.parquet");

/// Book-link record.
#[derive(Debug, TableRow, Serialize, Deserialize)]
pub struct BookLinkRecord {
    pub book_id: BookId,
    pub work_id: WorkId,
    pub cluster: i32,
}

/// Read a map of book IDs to linking identifiers.
pub fn load_id_links() -> Result<BookLinkMap> {
    let mut map = HashMap::with_capacity(1_000_000);
    let scan = scan_parquet_file(GR_LINK_FILE.resolve()?)?;

    for rec in scan {
        let rec: BookLinkRecord = rec?;
        map.insert(rec.book_id, rec);
    }

    info!("read {} book links from {}", map.len(), GR_LINK_FILE);
    Ok(map)
}
