//! Extract author information for book clusters.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use structopt::StructOpt;
use parse_display::{Display, FromStr};
use futures::{StreamExt};

use serde::{Serialize, Deserialize};

use datafusion::prelude::*;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use crate::prelude::*;
use crate::arrow::*;
use crate::arrow::fusion::*;
use crate::arrow::row_de::RecordBatchDeserializer;
use crate::cli::AsyncCommand;
use async_trait::async_trait;

#[derive(Display, FromStr, Debug)]
#[display(style="lowercase")]
enum Source {
  OpenLib,
  LOC,
}

#[derive(StructOpt, Debug)]
#[structopt(name="extract-authors")]
/// Extract cluster author data from extracted book data.
pub struct ClusterAuthors {
  /// Only extract first authors
  #[structopt(long="first-author")]
  first_author: bool,

  /// Specify output file
  #[structopt(short="o", long="output")]
  output: PathBuf,

  /// Specify the source
  #[structopt(short="s", long="source")]
  sources: Vec<Source>
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, ParquetRecordWriter, Default)]
struct ClusterAuthor {
  cluster: i32,
  author_name: String
}

/// Write out author records to file, without duplicates.
async fn write_authors_dedup<P: AsRef<Path>>(plan: Arc<dyn ExecutionPlan>, path: P) -> Result<()> {
  let mut writer = TableWriter::open(path)?;

  info!("scanning author batches");
  let stream = execute_stream(plan).await?;
  let mut last = ClusterAuthor::default();
  let mut rec_stream = RecordBatchDeserializer::for_stream(stream);
  while let Some(row) = rec_stream.next().await {
    let row: ClusterAuthor = row?;
    debug!("received batch");
    if row != last {
      writer.write_object(row.clone())?;
      last = row;
    }
  }

  let n = writer.finish()?;
  info!("wrote {} cluster-author links", n);

  Ok(())
}

/// Scan the OpenLibrary data for first authors
async fn scan_openlib(ctx: &mut ExecutionContext, first_only: bool) -> Result<Arc<dyn DataFrame>> {
  info!("scanning OpenLibrary author data");
  info!("reading ISBN clusters");
  let icl = ctx.read_parquet("book-links/isbn-clusters.parquet").await?;
  let icl = icl.select_columns(&["isbn_id", "cluster"])?;
  info!("reading edition IDs");
  let edl = ctx.read_parquet("openlibrary/edition-isbn-ids.parquet").await?;
  let edl = edl.filter(col("isbn_id").is_not_null())?;
  info!("reading edition authors");
  let mut eau = ctx.read_parquet("openlibrary/edition-authors.parquet").await?;
  if first_only {
    eau = eau.filter(col("pos").eq(lit(0)))?;
  }
  info!("reading author names");
  let auth = ctx.read_parquet("openlibrary/author-names.parquet").await?;
  let linked = icl.join(edl, JoinType::Inner, &["isbn_id"], &["isbn_id"])?;
  let linked = linked.join(eau, JoinType::Inner, &["edition"], &["edition"])?;
  let linked = linked.join(auth, JoinType::Inner, &["author"], &["id"])?;
  let authors = linked.select(vec![
    col("cluster"),
    trim(col("name")).alias("author_name")
  ])?;
  Ok(authors)
}

/// Scan the Library of Congress data for first authors.
async fn scan_loc(ctx: &mut ExecutionContext, first_only: bool) -> Result<Arc<dyn DataFrame>> {
  if !first_only {
    error!("only first-author extraction is currently supported");
    return Err(anyhow!("cannot extract multiple authors"));
  }

  info!("reading ISBN clusters");
  let icl = ctx.read_parquet("book-links/isbn-clusters.parquet").await?;
  let icl = icl.select_columns(&["isbn_id", "cluster"])?;

  info!("reading book records");
  let books = ctx.read_parquet("loc-mds/book-isbn-ids.parquet").await?;

  info!("reading book authors");
  let authors = ctx.read_parquet("loc-mds/book-authors.parquet").await?;
  let authors = authors.filter(col("author_name").is_not_null())?;

  let linked = icl.join(books, JoinType::Inner, &["isbn_id"], &["isbn_id"])?;
  let linked = linked.join(authors, JoinType::Inner, &["rec_id"], &["rec_id"])?;
  let authors = linked.select_columns(&["cluster", "author_name"])?;
  // we shouldn't have null author names, but the data thinks we do. fix.

  let authors = authors.filter(col("author_name").is_not_null())?;


  Ok(authors)
}

#[async_trait]
impl AsyncCommand for ClusterAuthors {
  async fn exec_future(&self) -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let mut authors: Option<Arc<dyn DataFrame>> = None;
    for source in &self.sources {
      let astr = match source {
        Source::OpenLib => scan_openlib(&mut ctx, self.first_author).await?,
        Source::LOC => scan_loc(&mut ctx, self.first_author).await?,
      };
      debug!("author source {} has schema {:?}", source, astr.schema());
      if let Some(adf) = authors {
        authors = Some(adf.union(astr)?);
      } else {
        authors = Some(astr);
      }
    }
    let authors = authors.ok_or(anyhow!("no sources specified"))?;
    let authors = authors.sort(vec![
      col("cluster").sort(true, true),
      col("author_name").sort(true, true)
      ])?;

    let plan = plan_df(&mut ctx, authors).await?;
    write_authors_dedup(plan, &self.output).await?;

    Ok(())
  }
}
