//! GoodReads review data model.
pub use serde::Deserialize;

use crate::arrow::*;
use crate::ids::index::IdIndex;
use crate::parsing::dates::*;
use crate::parsing::*;
use crate::prelude::*;

use super::ids::read_id_links;
use super::ids::BookId;
use super::ids::BookLinkMap;
use super::ids::WorkId;

const OUT_FILE: &'static str = "gr-reviews.parquet";

/// Review records we read from JSON.
#[derive(Deserialize)]
pub struct RawReview {
    pub user_id: String,
    pub book_id: String,
    pub review_id: String,
    pub rating: f32,
    pub review_text: String,
    pub n_votes: i32,
    pub date_added: String,
    pub date_updated: String,
    pub read_at: String,
    pub started_at: String,
}

/// Review records to write to the Parquet table.
#[derive(TableRow)]
pub struct ReviewRecord {
    /// Internal auto-genereated record identifier.
    pub rec_id: u32,
    /// Review identifier (derived from input).
    pub review_id: i64,
    /// User identifier.
    pub user_id: i32,
    /// GoodReads book identifier.
    pub book_id: BookId,
    /// GoodReads work identifier.
    pub work_id: WorkId,
    /// Cluster identifier (from [integration clustering][clust]).
    ///
    /// [clust]: https://bookdata.piret.info/data/cluster.html
    pub cluster: i32,
    /// Rating associated with this review (if provided).
    pub rating: Option<f32>,
    /// Review text.
    pub review: String,
    /// Number of votes this review has received.
    pub n_votes: i32,
    /// Date review was added.
    pub added: f32,
    /// Date review was updated.
    pub updated: f32,
}

// Object writer to transform and write GoodReads reviews
pub struct ReviewWriter {
    writer: TableWriter<ReviewRecord>,
    users: IdIndex<Vec<u8>>,
    books: BookLinkMap,
    n_recs: u32,
}

impl ReviewWriter {
    // Open a new output
    pub fn open() -> Result<ReviewWriter> {
        let writer = TableWriter::open(OUT_FILE)?;
        let books = read_id_links()?;
        Ok(ReviewWriter {
            writer,
            users: IdIndex::new(),
            books,
            n_recs: 0,
        })
    }
}

impl DataSink for ReviewWriter {
    fn output_files(&self) -> Vec<PathBuf> {
        path_list(&[OUT_FILE])
    }
}

impl ObjectWriter<RawReview> for ReviewWriter {
    // Write a single interaction to the output
    fn write_object(&mut self, row: RawReview) -> Result<()> {
        self.n_recs += 1;
        let rec_id = self.n_recs;
        let user_key = hex::decode(row.user_id.as_bytes())?;
        let user_id = self.users.intern_owned(user_key)?;
        let book_id: i32 = row.book_id.parse()?;
        let (rev_hi, rev_lo) = decode_hex_i64_pair(&row.review_id)?;
        let review_id = rev_hi ^ rev_lo;
        let link = self
            .books
            .get(&book_id)
            .ok_or_else(|| anyhow!("unknown book ID"))?;

        self.writer.write_object(ReviewRecord {
            rec_id,
            review_id,
            user_id,
            book_id,
            work_id: link.work_id,
            cluster: link.cluster,
            review: row.review_text,
            rating: if row.rating > 0.0 {
                Some(row.rating)
            } else {
                None
            },
            n_votes: row.n_votes,
            added: parse_gr_date(&row.date_added).map(check_ts("added", 2005))?,
            updated: parse_gr_date(&row.date_updated).map(check_ts("updated", 2005))?,
        })?;

        Ok(())
    }

    // Clean up and finalize output
    fn finish(self) -> Result<usize> {
        info!(
            "wrote {} records for {} users, closing output",
            self.n_recs,
            self.users.len()
        );
        self.writer.finish()
    }
}
