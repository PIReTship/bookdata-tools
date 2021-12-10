use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use datafusion::prelude::*;
use datafusion::logical_plan::Expr;

use crate::ids::codes::*;

#[async_trait]
pub trait EdgeRead: Debug {
  async fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>>;
}

#[async_trait]
pub trait NodeRead: Debug {
  async fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>>;
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

#[async_trait]
impl NodeRead for ISBN {
  async fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("book-links/all-isbns.parquet").await?;
    let df = df.select(vec![
      id_col("isbn_id", NS_ISBN).alias("code"),
      col("isbn").alias("label")
    ])?;
    Ok(df)
  }
}

#[async_trait]
impl NodeRead for LOC {
  async fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("loc-mds/book-ids.parquet").await?;
    let df = df.select(vec![
      id_col("rec_id", NS_LOC_REC).alias("code")
    ])?;
    Ok(df)
  }
}

#[async_trait]
impl EdgeRead for LOC {
  async fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("loc-mds/book-isbn-ids.parquet").await?;
    let df = df.select(vec![
      id_col("isbn_id", NS_ISBN),
      id_col("rec_id", NS_LOC_REC)
    ])?;
    Ok(df)
  }
}

#[async_trait]
impl NodeRead for OLEditions {
  async fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("openlibrary/editions.parquet").await?;
    let df = df.select(vec![
      id_col("id", NS_EDITION).alias("code")
    ])?;
    Ok(df)
  }
}

#[async_trait]
impl EdgeRead for OLEditions {
  async fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("openlibrary/edition-isbn-ids.parquet").await?;
    let df = df.select(vec![
      id_col("isbn_id", NS_ISBN),
      id_col("edition", NS_EDITION)
    ])?;
    Ok(df)
  }
}

#[async_trait]
impl NodeRead for OLWorks {
  async fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("openlibrary/all-works.parquet").await?;
    let df = df.select(vec![
      id_col("id", NS_WORK).alias("code"),
      col("key").alias("label")
    ])?;
    Ok(df)
  }
}

#[async_trait]
impl EdgeRead for OLWorks {
  async fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("openlibrary/edition-works.parquet").await?;
    let df = df.select(vec![
      id_col("edition", NS_EDITION),
      id_col("work", NS_WORK)
    ])?;
    Ok(df)
  }
}

#[async_trait]
impl NodeRead for GRBooks {
  async fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("goodreads/gr-book-ids.parquet").await?;
    let df = df.select(vec![
      id_col("book_id", NS_GR_BOOK).alias("code")
    ])?;
    Ok(df)
  }
}

#[async_trait]
impl EdgeRead for GRBooks {
  async fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("goodreads/book-isbn-ids.parquet").await?;
    let df = df.select(vec![
      id_col("isbn_id", NS_ISBN),
      id_col("book_id", NS_GR_BOOK)
    ])?;
    Ok(df)
  }
}

#[async_trait]
impl NodeRead for GRWorks {
  async fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("goodreads/gr-book-ids.parquet").await?;
    let df = df.filter(col("work_id").is_not_null())?;
    let df = df.select(vec![
      id_col("work_id", NS_GR_WORK).alias("code")
    ])?;
    Ok(df)
  }
}

#[async_trait]
impl EdgeRead for GRWorks {
  async fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("goodreads/gr-book-ids.parquet").await?;
    let df = df.filter(col("work_id").is_not_null())?;
    let df = df.select(vec![
      id_col("book_id", NS_GR_BOOK),
      id_col("work_id", NS_GR_WORK)
    ])?;
    Ok(df)
  }
}
