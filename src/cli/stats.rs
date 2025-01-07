//! Compute integration statistics.
use clap::Args;
use std::fs::File;

use crate::prelude::*;
use polars::prelude::*;

static GENDER_FILE: &str = "book-links/cluster-genders.parquet";
static ISBN_CLUSTER_FILE: &str = "book-links/isbn-clusters.parquet";
static LOC_BOOK_FILE: &str = "loc-mds/book-isbn-ids.parquet";
static STAT_FILE: &str = "book-links/gender-stats.csv";

static ACTION_FILES: &[(&str, &str)] = &[
    ("BX-I", "bx/bx-cluster-actions.parquet"),
    ("BX-E", "bx/bx-cluster-ratings.parquet"),
    ("AZ14", "az2014/az-cluster-ratings.parquet"),
    ("AZ18", "az2018/az-cluster-ratings.parquet"),
    ("GR-I", "goodreads/gr-cluster-actions.parquet"),
    ("GR-E", "goodreads/gr-cluster-ratings.parquet"),
];

/// Compute integration statistics.
#[derive(Debug, Args)]
#[command(name = "integration-stats")]
pub struct IntegrationStats {}

impl Command for IntegrationStats {
    fn exec(&self) -> Result<()> {
        require_working_root()?;
        let cfg = load_config()?;

        let genders = scan_genders()?;

        let loc_frame = scan_loc(genders.clone())?;
        let mut agg_frames = Vec::with_capacity(ACTION_FILES.len());

        for (name, file) in ACTION_FILES {
            if cfg.ds_enabled(name) {
                agg_frames.push(scan_actions(&file, genders.clone(), *name)?);
            }
        }

        info!("combining results");
        let mut results = agg_frames.into_iter().fold(Ok(loc_frame), |dfr, df2| {
            dfr.and_then(|df1| df1.vstack(&df2))
        })?;

        info!("saving {} records to {}", results.height(), STAT_FILE);
        let writer = File::create(STAT_FILE)?;
        let mut writer = CsvWriter::new(writer).include_header(true);
        writer.finish(&mut results)?;

        Ok(())
    }
}

fn scan_genders() -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet(GENDER_FILE, default())?;
    Ok(df)
}

fn scan_loc(genders: LazyFrame) -> Result<DataFrame> {
    info!("scanning LOC books");

    let books = LazyFrame::scan_parquet(LOC_BOOK_FILE, default())?;
    let clusters = LazyFrame::scan_parquet(ISBN_CLUSTER_FILE, default())?;
    let books = books.inner_join(clusters, col("isbn_id"), col("isbn_id"));

    let bg = books.inner_join(genders, col("cluster"), col("cluster"));
    let bg = bg
        .group_by([col("gender")])
        .agg([col("cluster").n_unique().alias("n_books")])
        .select([
            lit("LOC-MDS").alias("dataset"),
            col("gender"),
            col("n_books"),
            lit(NULL).cast(DataType::UInt32).alias("n_actions"),
        ]);

    let df = bg.collect()?;
    Ok(df)
}

fn scan_actions(file: &str, genders: LazyFrame, name: &str) -> Result<DataFrame> {
    info!("scanning data {} from {}", name, file);
    let df = LazyFrame::scan_parquet(file, default())?;

    let df = df.join(
        genders,
        &[col("item_id")],
        &[col("cluster")],
        JoinType::Inner.into(),
    );
    let df = df
        .group_by([col("gender")])
        .agg(&[
            col("item_id").n_unique().alias("n_books"),
            col("item_id").count().alias("n_actions"),
        ])
        .select([
            lit(name).alias("dataset"),
            col("gender"),
            col("n_books"),
            col("n_actions"),
        ]);
    debug!("{} schema: {:?}", name, df.schema());

    let df = df.collect()?;
    Ok(df)
}
