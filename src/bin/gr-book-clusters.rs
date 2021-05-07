use std::fs::File;


use polars::prelude::*;
use bookdata::prelude::*;

use anyhow::Result;

/// Attach GoodReads books to clusers.
#[derive(StructOpt)]
#[structopt(name="gr-book-clusters")]
pub struct BookClusters {
  #[structopt(flatten)]
  common: CommonOpts,
}

fn main() -> Result<()> {
  let options = BookClusters::from_args();
  options.common.init()?;

  info!("reading book IDs");
  let idr = File::open("gr-book-ids.parquet")?;
  let idr = ParquetReader::new(idr);
  let id_df = idr.finish()?;

  info!("reading ISBN IDs");
  let r = File::open("book-isbn-ids.parquet")?;
  let r = ParquetReader::new(r);
  let isbn_df = r.finish()?;

  info!("reading clusters");
  let r = File::open("../book-links/isbn-clusters.parquet")?;
  let r = ParquetReader::new(r);
  let clus_df = r.finish()?;

  info!("merging ISBNs");
  let merged = id_df.left_join(&isbn_df, "book_id", "book_id")?;
  let merged = merged.left_join(&clus_df, "isbn_id", "isbn_id")?;

  info!("deduplicating clusters");
  let subset = merged.select(&["book_id", "work_id", "cluster"])?;
  let mut subset = subset.drop_duplicates(false, None)?;

  info!("writing results");
  let w = File::create("gr-book-link.parquet")?;
  let mut w = ParquetWriter::new(w);
  w.finish(&mut subset)?;

  Ok(())
}
