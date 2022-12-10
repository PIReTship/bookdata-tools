//! Scan Amazon ratings.
use std::fs::File;
use csv;

use crate::prelude::*;
use crate::arrow::*;
use crate::ids::index::IdIndex;
use crate::amazon::*;
use crate::util::logging::data_progress;

/// Scan an Amazon rating CSV file into Parquet.
#[derive(Args, Debug)]
#[command(name="scan-ratings")]
pub struct ScanRatings {
  /// Swap user and item columns (for AZ 2018 data)
  #[arg(long="swap-id-columns")]
  swap_columns: bool,

  /// Rating output file
  #[arg(short='o', long="rating-output", name="FILE")]
  ratings_out: PathBuf,

  /// Input file
  #[arg(name = "INPUT")]
  infile: PathBuf,
}

impl Command for ScanRatings {
  fn exec(&self) -> Result<()> {
    info!("scanning Amazon rating CSV from {}", self.infile.display());
    let out = &self.ratings_out;
    info!("writing to {}", out.display());
    let mut writer = TableWriter::open(out)?;

    let src = File::open(&self.infile)?;
    let pb = data_progress(src.metadata()?.len());
    pb.set_prefix("ratings");
    let src = pb.wrap_read(src);
    let src = csv::ReaderBuilder::new().has_headers(false).from_reader(src);
    let src = src.into_deserialize();
    let mut index: IdIndex<String> = IdIndex::new();
    for row in src {
      let mut row: SourceRating = row?;
      if self.swap_columns {
        std::mem::swap(&mut row.user, &mut row.asin);
      }
      let user = index.intern(row.user.as_str())?;
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
}
