//! Compute integration statistics.
use std::fs::File;
use structopt::StructOpt;

use polars::prelude::*;
use crate::prelude::*;

static GENDER_FILE: &str = "book-links/cluster-genders.parquet";
static ISBN_CLUSTER_FILE: &str = "book-links/isbn-clusters.parquet";
static LOC_BOOK_FILE: &str = "loc-mds/book-isbn-ids.parquet";
static STAT_FILE: &str = "book-links/gender-stats.csv";

static ACTION_FILES: &[(&str, &str)] = &[
  ("BX-I", "bx/bx-cluster-actions.parquet"),
  ("BX-E", "bx/bx-cluster-ratings.parquet"),
  ("AZ", "az2014/az-cluster-ratings.parquet"),
  ("GR-I", "goodreads/${goodreads.interactions}/gr-cluster-actions.parquet"),
  ("GR-E", "goodreads/${goodreads.interactions}/gr-cluster-ratings.parquet"),
];

/// Compute integration statistics.
#[derive(Debug, StructOpt)]
#[structopt(name="integration-stats")]
pub struct IntegrationStats {
}

impl Command for IntegrationStats {
  fn exec(&self) -> Result<()> {
    require_working_root()?;
    let config = Config::load()?;

    let genders = scan_genders()?;

    let mut agg_frames = Vec::with_capacity(ACTION_FILES.len() + 1);
    agg_frames.push(scan_loc(genders.clone())?);

    for (name, file) in ACTION_FILES {
      let file = config.interpolate(file);
      agg_frames.push(scan_actions(&file, genders.clone(), *name)?);
    }

    let results = concat(agg_frames, true, true)?;
    info!("collecting results");
    debug!("plan: {:?}", results.describe_optimized_plan()?);
    let mut results = results.collect()?;

    info!("saving {} records to {}", results.height(), STAT_FILE);
    let writer = File::create(STAT_FILE)?;
    let mut writer = CsvWriter::new(writer).has_header(true);
    writer.finish(&mut results)?;

    Ok(())
  }
}

fn scan_genders() -> Result<LazyFrame> {
  let df = LazyFrame::scan_parquet(GENDER_FILE, default())?;
  Ok(df)
}

fn scan_loc(genders: LazyFrame) -> Result<LazyFrame> {
  info!("scanning LOC books");

  let books = LazyFrame::scan_parquet(LOC_BOOK_FILE, default())?;
  let clusters = LazyFrame::scan_parquet(ISBN_CLUSTER_FILE, default())?;
  let books = books.inner_join(clusters, col("isbn_id"), col("isbn_id"));

  let bg = books.inner_join(genders, col("cluster"), col("cluster"));
  let bg = bg.groupby([col("gender")]).agg([
    col("cluster").n_unique().alias("n_books"),
  ]).select([
    lit("LOC-MDS").alias("dataset"),
    col("gender"),
    col("n_books"),
    lit(NULL).cast(DataType::UInt32).alias("n_actions"),
  ]);

  Ok(bg)
}

fn scan_actions(file: &str, genders: LazyFrame, name: &str) -> Result<LazyFrame> {
  info!("scanning data {} from {}", name, file);
  let df = LazyFrame::scan_parquet(file, default())?;

  let df = df.join(genders, &[col("item")], &[col("cluster")], JoinType::Inner);
  let df = df.groupby([col("gender")]).agg(&[
    col("item").n_unique().alias("n_books"),
    col("item").count().alias("n_actions"),
  ]).select([
    lit(name).alias("dataset"),
    col("gender"),
    col("n_books"),
    col("n_actions"),
  ]);
  debug!("{} schema: {:?}", name, df.schema());

  Ok(df)
}
