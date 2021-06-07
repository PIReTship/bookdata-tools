//! Scan Amazon ratings or reviews.
use std::fs::File;
use csv;

use serde::{Serialize, Deserialize};

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::ids::index::IdIndex;

/// Scan an Amazon source file into Parquet.
#[derive(StructOpt)]
#[structopt(name="scan-interactions")]
pub struct ScanInteractions {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Rating output file
  #[structopt(short="o", long="rating-output", name="FILE", parse(from_os_str))]
  ratings_out: PathBuf,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf,
}

/// A rating in the source data format.
#[derive(Serialize, Deserialize)]
struct SourceRating {
  user: String,
  asin: String,
  rating: f32,
  timestamp: i64,
}

/// A rating in our post-scan format.
#[derive(TableRow)]
struct RatingRow {
  user: u32,
  asin: String,
  rating: f32,
  timestamp: i64,
}

fn scan_az_ratings(src: &Path, out: &Path) -> Result<()>
{
  info!("scanning Amazon rating CSV");
  info!("writing to {}", out.display());
  let mut writer = TableWriter::open(out)?;

  let src = File::open(src)?;
  let src = csv::ReaderBuilder::new().has_headers(false).from_reader(src);
  let src = src.into_deserialize();
  let mut timer = Timer::new();
  let mut index: IdIndex<String> = IdIndex::new();
  let iter = timer.iter_progress("reading ratings", 2.5, src);
  for row in iter {
    let row: SourceRating = row?;
    let user = index.intern(row.user.as_str());
    writer.write_object(RatingRow {
      user,
      asin: row.asin,
      rating: row.rating,
      timestamp: row.timestamp,
    })?;
  }

  writer.finish()?;
  Ok(())
}

fn main() -> Result<()> {
  let opts = ScanInteractions::from_args();
  opts.common.init()?;

  scan_az_ratings(opts.infile.as_ref(), opts.ratings_out.as_ref())?;

  Ok(())
}
