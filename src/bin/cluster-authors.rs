use std::path::{Path, PathBuf};
use std::collections::HashSet;
use std::sync::Arc;

use structopt::StructOpt;
use parse_display::{Display, FromStr};
use futures::{StreamExt};
use indicatif::ProgressBar;

use serde::{Serialize, Deserialize};

use tokio;

use happylog::set_progress;

use datafusion::prelude::*;
use datafusion::physical_plan::ExecutionPlan;
use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::arrow::fusion::*;
use bookdata::arrow::row_de::{RecordBatchDeserializer, BatchRecordIter};

#[derive(Display, FromStr, Debug)]
#[display(style="lowercase")]
enum Source {
  OpenLib,
  LOC,
}

#[derive(StructOpt, Debug)]
#[structopt(name="cluster-authors")]
/// Extract cluster author data from extracted book data.
struct ClusterAuthors {
  #[structopt(flatten)]
  common: CommonOpts,

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

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, TableRow)]
struct ClusterAuthor {
  cluster: i32,
  author_name: String
}

/// Write out author records to file, without duplicates.
async fn write_authors_dedup<P: AsRef<Path>>(plan: Arc<dyn ExecutionPlan>, path: P) -> Result<()> {
  let mut writer = TableWriter::open(path)?;

  let spin = ProgressBar::new_spinner();
  let _pbl = set_progress(&spin);
  spin.set_message("author batches");

  info!("scanning author batches");
  let mut stream = plan.execute(0).await?;
  let mut seen = HashSet::new();
  while let Some(batch) = stream.next().await {
    let batch = batch?;
    spin.tick();
    debug!("received batch");
    let rbd = RecordBatchDeserializer::new(batch)?;
    let mut rows: BatchRecordIter<ClusterAuthor> = rbd.iter();
    while let Some(row) = rows.next()? {
      if seen.insert(row.clone()) {
        // first time we saw it
        writer.write_object(row)?;
      }
    }
  }

  spin.finish_and_clear();

  let n = writer.finish()?;
  info!("wrote {} cluster-author links", n);

  Ok(())
}

/// Scan the OpenLibrary data for first authors
async fn scan_openlib(ctx: &mut ExecutionContext, first_only: bool) -> Result<Arc<dyn DataFrame>> {
  info!("scanning OpenLibrary author data");
  info!("reading ISBN clusters");
  let icl = ctx.read_parquet("book-links/isbn-clusters.parquet")?;
  info!("reading edition IDs");
  let edl = ctx.read_parquet("openlibrary/edition-isbn-ids.parquet")?;
  let edl = edl.filter(col("isbn_id").is_not_null())?;
  info!("reading edition authors");
  let mut eau = ctx.read_parquet("openlibrary/edition-authors.parquet")?;
  if first_only {
    eau = eau.filter(col("pos").eq(lit(0)))?;
  }
  info!("reading author names");
  let auth = ctx.read_parquet("openlibrary/author-names.parquet")?;
  let linked = icl.join(edl, JoinType::Inner, &["isbn_id"], &["isbn_id"])?;
  let linked = linked.join(eau, JoinType::Inner, &["edition"], &["edition"])?;
  let linked = linked.join(auth, JoinType::Inner, &["author"], &["id"])?;
  let authors = linked.select(vec![
    col("cluster"),
    // silly hack to make author names nullable and support union
    // nullif(&[col("name"), lit("")]).alias("author_name")
    col("name").alias("author_name")
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
  let icl = ctx.read_parquet("book-links/isbn-clusters.parquet")?;

  info!("reading book records");
  let books = ctx.read_parquet("loc-mds/book-isbn-ids.parquet")?;

  info!("reading book authors");
  let authors = ctx.read_parquet("loc-mds/book-authors.parquet")?;
  let authors = authors.filter(col("author_name").is_not_null())?;

  let linked = icl.join(books, JoinType::Inner, &["isbn_id"], &["isbn_id"])?;
  let linked = linked.join(authors, JoinType::Inner, &["rec_id"], &["rec_id"])?;
  let authors = linked.select_columns(&["cluster", "author_name"])?;
  // we shouldn't have null author names, but the data thinks we do. fix.

  let authors = authors.filter(col("author_name").is_not_null())?;


  Ok(authors)
}

#[tokio::main]
async fn main() -> Result<()> {
  let opts = ClusterAuthors::from_args();
  opts.common.init()?;

  let mut ctx = ExecutionContext::new();

  let mut authors: Option<Arc<dyn DataFrame>> = None;
  for source in &opts.sources {
    let astr = match source {
      Source::OpenLib => scan_openlib(&mut ctx, opts.first_author).await?,
      Source::LOC => scan_loc(&mut ctx, opts.first_author).await?,
    };
    debug!("author source {} has schema {:?}", source, astr.schema());
    if let Some(adf) = authors {
      authors = Some(adf.union(astr)?);
    } else {
      authors = Some(astr);
    }
  }
  let authors = authors.ok_or(anyhow!("no sources specified"))?;

  let plan = plan_df(&mut ctx, authors)?;
  write_authors_dedup(plan, opts.output).await?;

  Ok(())
}
