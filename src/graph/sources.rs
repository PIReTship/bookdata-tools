use std::fmt::Debug;

use anyhow::Result;

use polars::prelude::*;

use crate::ids::codes::*;
use crate::util::default;

pub trait EdgeRead: Debug {
  fn read_edges(&self) -> Result<LazyFrame>;
}

pub trait NodeRead: Debug {
  fn read_node_ids(&self) -> Result<LazyFrame>;
}

#[derive(Debug)]
pub struct ISBN;
#[derive(Debug)]
pub struct LOC;
#[derive(Debug)]
pub struct OLEditions;
#[derive(Debug)]
pub struct OLWorks;
#[derive(Debug)]
pub struct GRBooks;
#[derive(Debug)]
pub struct GRWorks;

/// Get an ID column and apply the appropriate namespace adjustment.
fn id_col(name: &str, ns: NS<'_>) -> Expr {
  col(name) + lit(ns.base())
}

impl NodeRead for ISBN {
  fn read_node_ids(&self) -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet("book-links/all-isbns.parquet".into(), default())?;
    let df = df.select([
      id_col("isbn_id", NS_ISBN).alias("code"),
      col("isbn").alias("label")
    ]);
    Ok(df)
  }
}

impl NodeRead for LOC {
  fn read_node_ids(&self) -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet("loc-mds/book-ids.parquet".into(), default())?;
    let df = df.select([
      id_col("rec_id", NS_LOC_REC).alias("code")
    ]);
    Ok(df)
  }
}

impl EdgeRead for LOC {
  fn read_edges(&self) -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet("loc-mds/book-isbn-ids.parquet".into(), default())?;
    let df = df.select([
      id_col("isbn_id", NS_ISBN).alias("src"),
      id_col("rec_id", NS_LOC_REC).alias("dst")
    ]);
    Ok(df)
  }
}

impl NodeRead for OLEditions {
  fn read_node_ids(&self) -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet("openlibrary/editions.parquet".into(), default())?;
    let df = df.select([
      id_col("id", NS_EDITION).alias("code")
    ]);
    Ok(df)
  }
}

impl EdgeRead for OLEditions {
  fn read_edges(&self) -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet("openlibrary/edition-isbn-ids.parquet".into(), default())?;
    let df = df.select([
      id_col("isbn_id", NS_ISBN).alias("src"),
      id_col("edition", NS_EDITION).alias("dst")
    ]);
    Ok(df)
  }
}

impl NodeRead for OLWorks {
  fn read_node_ids(&self) -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet("openlibrary/all-works.parquet".into(), default())?;
    let df = df.select([
      id_col("id", NS_WORK).alias("code"),
      col("key").alias("label")
    ]);
    Ok(df)
  }
}

impl EdgeRead for OLWorks {
  fn read_edges(&self) -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet("openlibrary/edition-works.parquet".into(), default())?;
    let df = df.select([
      id_col("edition", NS_EDITION).alias("src"),
      id_col("work", NS_WORK).alias("dst")
    ]);
    Ok(df)
  }
}

impl NodeRead for GRBooks {
  fn read_node_ids(&self) -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet("goodreads/gr-book-ids.parquet".into(), default())?;
    let df = df.select([
      id_col("book_id", NS_GR_BOOK).alias("code")
    ]);
    Ok(df)
  }
}

impl EdgeRead for GRBooks {
  fn read_edges(&self) -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet("goodreads/book-isbn-ids.parquet".into(), default())?;
    let df = df.select([
      id_col("isbn_id", NS_ISBN).alias("src"),
      id_col("book_id", NS_GR_BOOK).alias("dst")
    ]);
    Ok(df)
  }
}

impl NodeRead for GRWorks {
  fn read_node_ids(&self) -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet("goodreads/gr-book-ids.parquet".into(), default())?;
    let df = df.filter(col("work_id").is_not_null());
    let df = df.select([
      id_col("work_id", NS_GR_WORK).alias("code")
    ]);
    Ok(df)
  }
}

impl EdgeRead for GRWorks {
  fn read_edges(&self) -> Result<LazyFrame> {
    let df = LazyFrame::scan_parquet("goodreads/gr-book-ids.parquet".into(), default())?;
    let df = df.filter(col("work_id").is_not_null());
    let df = df.select([
      id_col("book_id", NS_GR_BOOK).alias("src"),
      id_col("work_id", NS_GR_WORK).alias("dst")
    ]);
    Ok(df)
  }
}
