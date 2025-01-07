//! Scan Amazon reviews.
use crate::amazon::*;
use crate::arrow::*;
use crate::ids::index::IdIndex;
use crate::prelude::*;
use crate::util::logging::data_progress;

/// Scan an Amazon review JSON file into Parquet.
#[derive(Args, Debug)]
#[command(name = "scan-reviews")]
pub struct ScanReviews {
    /// Rating output file
    #[arg(short = 'o', long = "rating-output")]
    ratings_out: PathBuf,

    /// Review output file
    #[arg(short = 'r', long = "review-output")]
    reviews_out: Option<PathBuf>,

    /// Input file
    #[arg(name = "INPUT")]
    infile: PathBuf,
}

impl Command for ScanReviews {
    fn exec(&self) -> Result<()> {
        info!("scanning Amazon reviews");

        let out = &self.ratings_out;
        info!("writing ratings to {}", out.display());
        let mut ratings = TableWriter::open(out)?;

        let mut reviews = if let Some(ref p) = self.reviews_out {
            info!("writing reviews to {}", p.display());
            Some(TableWriter::open(p)?)
        } else {
            None
        };

        let pb = data_progress(0);
        let src = LineProcessor::open_gzip(&self.infile, pb.clone())?;
        // let mut timer = Timer::new();
        let mut users: IdIndex<String> = IdIndex::new();
        let mut lno: usize = 0;
        // let iter = timer.iter_progress("reading reviews", 5.0, src.json_records());
        for row in src.json_records() {
            lno += 1;
            let row: SourceReview = row.map_err(|e| {
                error!("parse error on line {}: {}", lno, e);
                e
            })?;
            let user_id = users.intern(row.user.as_str())?;
            ratings.write_object(RatingRow {
                user_id,
                asin: row.asin.clone(),
                rating: row.rating,
                timestamp: row.timestamp,
            })?;

            if let Some(ref mut rvw) = reviews {
                if row.summary.is_some() || row.text.is_some() {
                    rvw.write_object(ReviewRow {
                        user_id,
                        asin: row.asin,
                        rating: row.rating,
                        timestamp: row.timestamp,
                        summary: row.summary.unwrap_or_default().trim().to_owned(),
                        text: row.text.unwrap_or_default().trim().to_owned(),
                    })?;
                }
            }
        }

        ratings.finish()?;
        if let Some(rvw) = reviews {
            rvw.finish()?;
        }
        Ok(())
    }
}
