use structopt::StructOpt;

use polars::prelude::*;

use crate::prelude::*;
use crate::ids::codes::*;

static ALL_ISBNS_FILE: &str = "../book-links/all-isbns.parquet";
static GRAPH_NODE_FILE: &str = "../book-links/cluster-graph-nodes.parquet";
static BOOK_ID_FILE: &str = "gr-book-ids.parquet";
static BOOK_ISBN_ID_FILE: &str = "book-isbn-ids.parquet";
static BOOK_LINK_FILE: &str = "gr-book-link.parquet";

#[derive(StructOpt, Debug)]
pub enum GRLink {
  /// Link book clusters.
  #[structopt(name="clusters")]
  Clusters,
}

impl GRLink {
  pub fn exec(&self) -> Result<()> {
    match self {
      GRLink::Clusters => link_clusters(),
    }
  }
}

fn link_clusters() -> Result<()> {
  require_working_dir("goodreads")?;

  let clusters = LazyFrame::scan_parquet(GRAPH_NODE_FILE, default())?;
  let books = LazyFrame::scan_parquet(BOOK_ID_FILE, default())?;

  let clusters = clusters.filter(
    (col("book_code") / lit(NS_MULT_BASE)).eq(lit(NS_GR_BOOK.code()))
  ).select(&[
    (col("book_code") - lit(NS_GR_BOOK.base())).alias("book_id"),
    col("cluster"),
  ]);

  let links = books.join(clusters, &[col("book_id")], &[col("book_id")], JoinType::Inner);
  let links = links.select(&[
    col("book_id"),
    col("work_id"),
    col("cluster"),
  ]);

  info!("collecting results");
  let links = links.collect()?;

  info!("writing {} book link records", links.height());
  save_df_parquet(links, BOOK_LINK_FILE)?;


  Ok(())
}
