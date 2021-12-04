//! Read cluster information.
use std::path::Path;
use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use fallible_iterator::FallibleIterator;

use crate::prelude::*;
use crate::gender::*;
use crate::arrow::row_de::scan_parquet_file;
use super::authors::AuthorTable;

/// Record for storing a cluster's gender statistics while aggregating.
#[derive(Debug, Default)]
pub struct ClusterStats {
  pub n_book_authors: u32,
  pub n_author_recs: u32,
  pub n_gender_recs: u32,
  pub genders: HashSet<Gender>,
}

/// Row struct for reading cluster author names.
#[derive(Debug, Deserialize)]
struct ClusterAuthor {
  cluster: i32,
  author_name: String,
}

/// Row struct for reading cluster IDs.
#[derive(Debug, Deserialize)]
struct Cluster {
  cluster: i32,
}

pub type ClusterTable = HashMap<i32,ClusterStats>;

/// Read cluster author names and resolve them to gender information.
pub fn read_resolve<P: AsRef<Path>>(path: P, authors: &AuthorTable) -> Result<ClusterTable> {
  let path = path.as_ref();
  let timer = Timer::builder().label("reading cluster authors").interval(5.0).build();
  info!("reading cluster authors from {}", path.display());
  let iter = scan_parquet_file(path)?;
  let mut iter = timer.copy_builder().fallible_iter_progress(iter);

  let mut table = ClusterTable::new();

  while let Some(row) = iter.next()? {
    let row: ClusterAuthor = row;
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
  let mut iter = scan_parquet_file(path)?;
  let mut ids = Vec::new();
  while let Some(row) = iter.next()? {
    let row: Cluster = row;
    ids.push(row.cluster);
  }

  info!("found {} cluster IDs", ids.len());

  Ok(ids)
}
