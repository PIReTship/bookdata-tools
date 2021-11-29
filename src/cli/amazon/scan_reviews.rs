//! Scan Amazon reviews.
use crate::prelude::*;
use crate::arrow::*;
use crate::ids::index::IdIndex;
use crate::amazon::*;

/// Scan an Amazon review JSON file into Parquet.
#[derive(StructOpt, Debug)]
#[structopt(name="scan-reviews")]
pub struct ScanReviews {
  /// Rating output file
  #[structopt(short="o", long="rating-output", name="FILE", parse(from_os_str))]
  ratings_out: PathBuf,

  /// Review output file
  #[structopt(short="r", long="review-output", name="FILE", parse(from_os_str))]
  reviews_out: Option<PathBuf>,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
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

    let src = LineProcessor::open_gzip(&self.infile)?;
    let mut timer = Timer::new();
    let mut users: IdIndex<String> = IdIndex::new();
    let iter = timer.iter_progress("reading ratings", 2.5, src.json_records());
    for row in iter {
      let row: SourceReview = row?;
      let user = users.intern(row.user.as_str());
      ratings.write_object(RatingRow {
        user,
        asin: row.asin.clone(),
        rating: row.rating,
        timestamp: row.timestamp,
      })?;

      if let Some(ref mut rvw) = reviews {
        rvw.write_object(ReviewRow {
          user, asin: row.asin, rating: row.rating, timestamp: row.timestamp,
          summary: row.summary,
          text: row.text,
        })?;
      }
    }

    ratings.finish()?;
    if let Some(rvw) = reviews {
      rvw.finish()?;
    }
    Ok(())
  }
}
