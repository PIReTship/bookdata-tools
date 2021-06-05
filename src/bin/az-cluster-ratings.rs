//! Scan Amazon ratings or reviews.
use std::fs::File;
use csv;

use serde::{Serialize, Deserialize};
use chrono::NaiveDateTime;

use polars::prelude::*;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::ids::index::IdIndex;

use anyhow::Result;

/// Scan an Amazon source file into Parquet.
#[derive(StructOpt)]
#[structopt(name="az-cluster-ratings")]
pub struct ClusterRatings {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Rating output file
  #[structopt(short="o", long="output", name="FILE", parse(from_os_str))]
  outfile: PathBuf,

  /// Rating input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf,
}

fn main() -> Result<()> {
  let opts = ClusterRatings::from_args();
  opts.common.init()?;

  info!("reading ISBNs");
  let isbns = File::open("book-links/all-isbns.parquet")?;
  let isbns = ParquetReader::new(isbns).finish()?;

  info!("reading clusters");
  let clusters = File::open("book-links/isbn-clusters.parquet")?;
  let clusters = ParquetReader::new(clusters).finish()?;

  info!("reading ratings from {}", opts.infile.display());
  let ratings = File::open(&opts.infile)?;
  let ratings = ParquetReader::new(ratings).finish()?;

  Ok(())
}
