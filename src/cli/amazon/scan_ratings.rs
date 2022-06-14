//! Scan Amazon ratings.
use std::fs::File;
use csv;

use crate::prelude::*;
use crate::arrow::*;
use crate::ids::index::IdIndex;
use crate::amazon::*;
use crate::util::logging::data_progress;

/// Scan an Amazon rating CSV file into Parquet.
#[derive(StructOpt, Debug)]
#[structopt(name="scan-ratings")]
pub struct ScanRatings {
  /// Rating output file
  #[structopt(short="o", long="rating-output", name="FILE", parse(from_os_str))]
  ratings_out: PathBuf,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf,
}

impl Command for ScanRatings {
  fn exec(&self) -> Result<()> {
    info!("scanning Amazon rating CSV");
    let out = &self.ratings_out;
    info!("writing to {}", out.display());
    let mut writer = TableWriter::open(out)?;

    let src = File::open(&self.infile)?;
    let pb = data_progress(src.metadata()?.len());
    pb.set_prefix(self.infile.to_string_lossy().to_string());
    let _lg = set_progress(pb.clone());
    let src = pb.wrap_read(src);
    let src = csv::ReaderBuilder::new().has_headers(false).from_reader(src);
    let src = src.into_deserialize();
    let mut index: IdIndex<String> = IdIndex::new();
    for row in src {
      let row: SourceRating = row?;
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
