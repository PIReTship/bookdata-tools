//! Support for loading author info.
use std::collections::{HashMap, HashSet};

use crate::prelude::*;
use crate::gender::*;
use crate::arrow::scan_parquet_file;
use crate::util::logging::item_progress;

#[derive(Debug, Default)]
pub struct AuthorInfo {
  pub n_author_recs: u32,
  pub genders: HashSet<Gender>
}

pub type AuthorTable = HashMap<String,AuthorInfo>;

#[derive(ArrowField, Debug)]
struct NameRow {
  rec_id: u32,
  name: String,
}

#[derive(ArrowField, Debug)]
struct GenderRow {
  rec_id: u32,
  gender: String
}

/// Load VIAF author names.
fn viaf_load_names() -> Result<HashMap<u32, Vec<String>>> {
  let mut map: HashMap<u32, Vec<String>> = HashMap::new();

  info!("loading VIAF author names");
  let iter = scan_parquet_file("viaf/author-name-index.parquet")?;

  let pb = item_progress(iter.remaining() as u64, "authors");
  let _lg = set_progress(pb.clone());
  let iter = pb.wrap_iter(iter);
  let timer = Timer::new();

  for row in iter {
    let row: NameRow = row?;
    map.entry(row.rec_id).or_default().push(row.name);
  }

  info!("loaded authors for {} records in {}", map.len(), timer.human_elapsed());

  Ok(map)
}

/// Load VIAF author genders
fn viaf_load_genders() -> Result<HashMap<u32, HashSet<Gender>>> {
  let mut map: HashMap<u32, HashSet<Gender>> = HashMap::new();
  let timer = Timer::new();

  info!("loading VIAF author genders");
  let iter = scan_parquet_file("viaf/author-genders.parquet")?;

  let pb = item_progress(iter.remaining(), "authors");
  let _lg = set_progress(pb.clone());
  let iter = pb.wrap_iter(iter);

  for row in iter {
    let row: GenderRow = row?;
    let gender: Gender = row.gender.into();
    map.entry(row.rec_id).or_default().insert(gender);
  }

  info!("loaded genders for {} records in {}", map.len(), timer.human_elapsed());

  Ok(map)
}

/// Load the VIAF author gender records.
pub fn viaf_author_table() -> Result<AuthorTable> {
  let mut table = AuthorTable::new();

  let rec_names = viaf_load_names()?;
  let rec_genders = viaf_load_genders()?;
  let empty = HashSet::new();

  info!("merging gender records");
  let pb = item_progress(rec_names.len() as u64, "clusters");
  let _lg = set_progress(pb.clone());
  let timer = Timer::new();
  for (rec_id, names) in pb.wrap_iter(rec_names.into_iter()) {
    let genders = rec_genders.get(&rec_id).unwrap_or(&empty);
    for name in names {
      let mut rec = table.entry(name).or_default();
      rec.n_author_recs += 1;
      for g in genders {
        rec.genders.insert(g.clone());
      }
    }
  }

  info!("merged {} gender records in {}", table.len(), timer);

  Ok(table)
}
