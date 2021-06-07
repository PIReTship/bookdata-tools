use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;

use datafusion::prelude::*;
use datafusion::logical_plan::Expr;

use crate::ids::codes::*;

pub trait EdgeRead: Debug {
  fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>>;
}

pub trait NodeRead: Debug {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>>;
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

pub fn edge_sources() -> Vec<Box<dyn EdgeRead>> {
  vec![
    Box::new(LOC),
    Box::new(OLEditions),
    Box::new(OLWorks),
    Box::new(GRBooks),
    Box::new(GRWorks)
  ]
}

#[allow(dead_code)]
pub fn node_sources() -> Vec<Box<dyn NodeRead>> {
  vec![
    Box::new(ISBN),
    Box::new(LOC),
    Box::new(OLEditions),
    Box::new(OLWorks),
    Box::new(GRBooks),
    Box::new(GRWorks)
  ]
}

/// Get an ID column and apply the appropriate namespace adjustment.
fn id_col(name: &str, ns: NS<'_>) -> Expr {
  col(name) + lit(ns.base())
}

impl NodeRead for ISBN {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("book-links/all-isbns.parquet")?;
    let df = df.select(vec![
      id_col("isbn_id", NS_ISBN).alias("code"),
      col("isbn").alias("label")
    ])?;
    Ok(df)
  }
}

impl NodeRead for LOC {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("loc-mds/book-ids.parquet")?;
    let df = df.select(vec![
      id_col("rec_id", NS_LOC_REC).alias("code")
    ])?;
    Ok(df)
  }
}

impl EdgeRead for LOC {
  fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("loc-mds/book-isbn-ids.parquet")?;
    let df = df.select(vec![
      id_col("isbn_id", NS_ISBN),
      id_col("rec_id", NS_LOC_REC)
    ])?;
    Ok(df)
  }
}

impl NodeRead for OLEditions {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("openlibrary/editions.parquet")?;
    let df = df.select(vec![
      id_col("id", NS_EDITION).alias("code")
    ])?;
    Ok(df)
  }
}

impl EdgeRead for OLEditions {
  fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("openlibrary/edition-isbn-ids.parquet")?;
    let df = df.select(vec![
      id_col("isbn_id", NS_ISBN),
      id_col("edition", NS_EDITION)
    ])?;
    Ok(df)
  }
}

impl NodeRead for OLWorks {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("openlibrary/all-works.parquet")?;
    let df = df.select(vec![
      id_col("id", NS_WORK).alias("code")
    ])?;
    Ok(df)
  }
}

impl EdgeRead for OLWorks {
  fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("openlibrary/edition-works.parquet")?;
    let df = df.select(vec![
      id_col("edition", NS_EDITION),
      id_col("work", NS_WORK)
    ])?;
    Ok(df)
  }
}

impl NodeRead for GRBooks {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("goodreads/gr-book-ids.parquet")?;
    let df = df.select(vec![
      id_col("book_id", NS_GR_BOOK).alias("code")
    ])?;
    Ok(df)
  }
}

impl EdgeRead for GRBooks {
  fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("goodreads/book-isbn-ids.parquet")?;
    let df = df.select(vec![
      id_col("isbn_id", NS_ISBN),
      id_col("book_id", NS_GR_BOOK)
    ])?;
    Ok(df)
  }
}

impl NodeRead for GRWorks {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("goodreads/gr-book-ids.parquet")?;
    let df = df.filter(col("work_id").is_not_null())?;
    let df = df.select(vec![
      id_col("work_id", NS_GR_WORK).alias("code")
    ])?;
    Ok(df)
  }
}

impl EdgeRead for GRWorks {
  fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("goodreads/gr-book-ids.parquet")?;
    let df = df.filter(col("work_id").is_not_null())?;
    let df = df.select(vec![
      id_col("book_id", NS_GR_BOOK),
      id_col("work_id", NS_GR_WORK)
    ])?;
    Ok(df)
  }
}
