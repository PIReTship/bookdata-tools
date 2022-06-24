//! Read cluster information.
use std::convert::identity;
use std::path::Path;
use std::collections::{HashMap, HashSet};

use arrow2_convert::field::LargeString;
use polars::prelude::*;
use crate::prelude::*;
use crate::gender::*;
use crate::arrow::scan_parquet_file;
use crate::util::logging::item_progress;
use super::authors::AuthorTable;
use anyhow::Result;

/// Record for storing a cluster's gender statistics while aggregating.
#[derive(Debug, Default)]
pub struct ClusterStats {
  pub n_book_authors: u32,
  pub n_author_recs: u32,
  pub n_gender_recs: u32,
  pub genders: HashSet<Gender>,
}

/// Row struct for reading cluster author names.
#[derive(Debug, ArrowField)]
struct ClusterAuthor {
  cluster: i32,
  #[arrow_field(override="LargeString")]
  author_name: String,
}

pub type ClusterTable = HashMap<i32,ClusterStats>;

/// Read cluster author names and resolve them to gender information.
pub fn read_resolve(path: &Path, authors: &AuthorTable) -> Result<ClusterTable> {
  let timer = Timer::new();
  info!("reading cluster authors from {}", path.display());
  let iter = scan_parquet_file(path)?;

  let pb = item_progress(iter.remaining() as u64, "authors");
  pb.set_draw_delta(5000);
  let _lg = set_progress(pb.clone());

  let mut table = ClusterTable::new();

  for row in pb.wrap_iter(iter) {
    let row: ClusterAuthor = row?;
    let mut rec = table.entry(row.cluster).or_default();
    rec.n_book_authors += 1;
    if let Some(info) = authors.get(row.author_name.as_str()) {
      rec.n_author_recs += info.n_author_recs;
      rec.n_gender_recs += info.genders.len() as u32;
      for g in &info.genders {
        rec.genders.insert(g.clone());
      }
    }
  }

  info!("scanned genders for {} clusters in {}", table.len(), timer.human_elapsed());

  Ok(table)
}

/// Read the full list of cluster IDs.
pub fn all_clusters<P: AsRef<Path>>(path: P) -> Result<Vec<i32>> {
  info!("reading cluster IDs from {}", path.as_ref().display());
  let path = path.as_ref().to_str().map(|s| s.to_string()).ok_or(anyhow!("invalid unicode path"))?;
  let df = LazyFrame::scan_parquet(path, Default::default())?;
  let df = df.select([col("cluster")]);
  let clusters = df.collect()?;
  let ids = clusters.column("cluster")?.i32()?;

  info!("found {} cluster IDs", ids.len());

  Ok(ids.into_iter().filter_map(identity).collect())
}
