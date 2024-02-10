//! GoodReads book identifier and linking support.
use std::{collections::HashMap, fs::File};

use anyhow::Result;
use log::*;
use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::prelude::BDPath;

pub type BookLinkMap = HashMap<i32, BookLinkRecord>;

const GR_LINK_FILE: BDPath<'static> = BDPath::new("goodreads/gr-book-link.parquet");

/// Book-link record.
#[derive(Debug, Serialize, Deserialize)]
pub struct BookLinkRecord {
    pub book_id: i32,
    pub work_id: Option<i32>,
    pub cluster: i32,
}

/// Read a map of book IDs to linking identifiers.
pub fn load_id_links() -> Result<BookLinkMap> {
    let path = GR_LINK_FILE.resolve()?;
    let file = File::open(path)?;
    let pqf = ParquetReader::new(file);
    let df = pqf.finish()?;

    let mut map = HashMap::with_capacity(df.height());

    let c_book = df.column("book_id")?.i32()?;
    let c_work = df.column("work_id")?.i32()?;
    let c_cluster = df.column("cluster")?.i32()?;

    for i in 0..df.height() {
        let rec: BookLinkRecord = BookLinkRecord {
            book_id: c_book.get(i).unwrap(),
            work_id: c_work.get(i),
            cluster: c_cluster.get(i).unwrap(),
        };
        map.insert(rec.book_id, rec);
    }

    info!("read {} book links from {}", map.len(), GR_LINK_FILE);
    Ok(map)
}
