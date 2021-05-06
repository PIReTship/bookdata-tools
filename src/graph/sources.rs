use std::fmt::Debug;
use std::sync::Arc;

use log::*;
use anyhow::Result;

use datafusion::prelude::*;

use crate::ids::codes::*;

pub trait QueryContext {
  fn isbn_table(&self) -> String;
  fn node_limit(&self) -> String;
}

pub struct FullTable;

pub trait EdgeRead: Debug {
  fn read_edges(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>>;
}

pub trait EdgeQuery<Q: QueryContext>: Debug {
  fn q_edges(&self, qc: &Q) -> String;
}

pub trait NodeRead: Debug {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>>;
}

pub trait NodeQuery<Q: QueryContext>: Debug {
  fn q_node_ids(&self, qc: &Q) -> String;
  fn q_node_full(&self, qc: &Q) -> String;
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

pub fn edge_sources<Q: QueryContext>() -> Vec<Box<dyn EdgeQuery<Q>>> {
  vec![
    Box::new(LOC),
    Box::new(OLEditions),
    Box::new(OLWorks),
    Box::new(GRBooks),
    Box::new(GRWorks)
  ]
}

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

impl QueryContext for FullTable {
  fn isbn_table(&self) -> String {
    "isbn_id".to_owned()
  }

  fn node_limit(&self) -> String {
    "".to_owned()
  }
}

impl NodeRead for ISBN {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("book-links/all-isbns.parquet")?;
    let df = df.select(vec![
      col("isbn_id") + lit(NS_ISBN.base())
    ])?;
    Ok(df)
  }
}

impl <Q: QueryContext> NodeQuery<Q> for ISBN {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("SELECT bc_of_isbn(isbn_id) AS id FROM {}", qc.isbn_table())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("SELECT bc_of_isbn(isbn_id) AS id, isbn AS label FROM {}", qc.isbn_table())
  }
}

impl NodeRead for LOC {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("loc-mds/book-ids.parquet")?;
    let df = df.select(vec![
      col("rec_id") + lit(NS_LOC_REC.base())
    ])?;
    Ok(df)
  }
}

impl <Q: QueryContext> NodeQuery<Q> for LOC {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_loc_rec(rec_id) AS id
      FROM locmds.book_rec_isbn {}
    ", qc.node_limit())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_loc_rec(rec_id) AS id, title
      FROM locmds.book_rec_isbn {}
      LEFT JOIN locmds.book_title USING (rec_id)
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> EdgeQuery<Q> for LOC {
  fn q_edges(&self, qc: &Q) -> String {
    format!("
      SELECT bc_of_isbn(isbn_id) AS src, bc_of_loc_rec(rec_id) AS dst
      FROM locmds.book_rec_isbn {}
    ", qc.node_limit())
  }
}

impl NodeRead for OLEditions {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("openlibrary/editions.parquet")?;
    let df = df.select(vec![
      col("id") + lit(NS_EDITION.base())
    ])?;
    Ok(df)
  }
}

impl <Q: QueryContext> NodeQuery<Q> for OLEditions {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_edition(edition_id) AS id
      FROM ol.isbn_link {}
    ", qc.node_limit())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT
          bc_of_edition(edition_id) AS id, edition_key AS label,
          NULLIF(edition_data->>'title', '') AS title
      FROM ol.isbn_link {}
      JOIN ol.edition USING (edition_id)
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> EdgeQuery<Q> for OLEditions {
  fn q_edges(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_isbn(isbn_id) AS src, bc_of_edition(edition_id) AS dst
      FROM ol.isbn_link {}
    ", qc.node_limit())
  }
}

impl NodeRead for OLWorks {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("openlibrary/all-works.parquet")?;
    let df = df.select(vec![
      col("id") + lit(NS_WORK.base())
    ])?;
    Ok(df)
  }
}

impl <Q: QueryContext> NodeQuery<Q> for OLWorks {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_work(work_id) AS id
      FROM ol.isbn_link {}
      WHERE work_id IS NOT NULL
    ", qc.node_limit())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT
          bc_of_work(work_id) AS id, work_key AS label,
          NULLIF(work_data->>'title', '') AS title
      FROM ol.isbn_link {}
      JOIN ol.work USING (work_id)
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> EdgeQuery<Q> for OLWorks {
  fn q_edges(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_edition(edition_id) AS src, bc_of_work(work_id) AS dst
      FROM ol.isbn_link {}
      WHERE work_id IS NOT NULL
    ", qc.node_limit())
  }
}

impl NodeRead for GRBooks {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("goodreads/gr-book-ids.parquet")?;
    let df = df.select(vec![
      col("book_id") + lit(NS_GR_BOOK.base())
    ])?;
    Ok(df)
  }
}

impl <Q: QueryContext> NodeQuery<Q> for GRBooks {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_gr_book(gr_book_id) AS id
      FROM gr.book_isbn {}
    ", qc.node_limit())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_gr_book(gr_book_id) AS id
      FROM gr.book_isbn {}
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> EdgeQuery<Q> for GRBooks {
  fn q_edges(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_isbn(isbn_id) AS src, bc_of_gr_book(gr_book_id) AS dst
      FROM gr.book_isbn {}
    ", qc.node_limit())
  }
}

impl NodeRead for GRWorks {
  fn read_node_ids(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    let df = ctx.read_parquet("goodreads/gr-book-ids.parquet")?;
    let df = df.filter(col("work_id").is_not_null())?;
    let df = df.select(vec![
      col("work_id") + lit(NS_GR_WORK.base())
    ])?;
    Ok(df)
  }
}

impl <Q: QueryContext> NodeQuery<Q> for GRWorks {
  fn q_node_ids(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_gr_work(gr_work_id) AS id
      FROM gr.book_isbn {}
      JOIN gr.book_ids ids USING (gr_book_id)
      WHERE ids.gr_work_id IS NOT NULL
    ", qc.node_limit())
  }

  fn q_node_full(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_gr_work(gr_work_id) AS id, work_title AS title
      FROM gr.book_isbn {}
      JOIN gr.book_ids ids USING (gr_book_id)
      LEFT JOIN gr.work_title USING (gr_work_id)
      WHERE ids.gr_work_id IS NOT NULL
    ", qc.node_limit())
  }
}

impl <Q: QueryContext> EdgeQuery<Q> for GRWorks {
  fn q_edges(&self, qc: &Q) -> String {
    format!("
      SELECT DISTINCT bc_of_gr_book(gr_book_id) AS src, bc_of_gr_work(gr_work_id) AS dst
      FROM gr.book_isbn {}
      JOIN gr.book_ids ids USING (gr_book_id)
      WHERE ids.gr_work_id IS NOT NULL
    ", qc.node_limit())
  }
}
