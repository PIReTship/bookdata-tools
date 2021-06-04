//! Support for loading author info.
use std::collections::{HashMap, HashSet};

use serde::{Deserialize};

use bookdata::prelude::*;
use bookdata::gender::*;
use bookdata::arrow::row_de::scan_parquet_file;

#[derive(Debug, Default)]
pub struct AuthorInfo {
  n_author_recs: u32,
  genders: HashSet<Gender>
}

pub type AuthorTable = HashMap<String,AuthorInfo>;

#[derive(Deserialize, Debug)]
struct NameRow {
  rec_id: u32,
  name: String,
}

#[derive(Deserialize, Debug)]
struct GenderRow {
  rec_id: u32,
  gender: String
}

/// Load VIAF author names.
fn viaf_load_names() -> Result<HashMap<u32, Vec<String>>> {
  let mut map: HashMap<u32, Vec<String>> = HashMap::new();

  info!("loading VIAF author names");
  let iter = scan_parquet_file("viaf/author-name-index.parquet")?;

  let mut timer = Timer::new();
  let mut iter = timer.fallible_iter_progress("reading author names", 5.0, iter);

  while let Some(row) = iter.next()? {
    let row: NameRow = row;
    map.entry(row.rec_id).or_default().push(row.name);
  }

  info!("loaded authors for {} records in {}", map.len(), timer.human_elapsed());

  Ok(map)
}

/// Load VIAF author genders
fn viaf_load_genders() -> Result<HashMap<u32, HashSet<Gender>>> {
  let mut map: HashMap<u32, HashSet<Gender>> = HashMap::new();
  let mut timer = Timer::new();

  info!("loading VIAF author genders");
  let iter = scan_parquet_file("viaf/author-genders.parquet")?;
  let mut iter = timer.fallible_iter_progress("reading author genders", 5.0, iter);
  while let Some(row) = iter.next()? {
    let row: GenderRow = row;
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
  let timer = Timer::new();
  for (rec_id, names) in rec_names {
    let genders = rec_genders.get(&rec_id).unwrap_or(&empty);
    for name in names {
      let mut rec = table.entry(name).or_default();
      rec.n_author_recs += 1;
      for g in genders {
        rec.genders.insert(g.clone());
      }
    }
  }

  info!("read {} gender records in {}", table.len(), timer);

  Ok(table)
}
