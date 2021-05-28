//! Support for loading author info.
use std::path::{Path, PathBuf};
use std::collections::{HashMap,HashSet};
use std::sync::Arc;

use structopt::StructOpt;
use futures::{StreamExt};

use serde::{Serialize, Deserialize};

use tokio;

use arrow::datatypes::*;
use datafusion::prelude::*;
use datafusion::physical_plan::ExecutionPlan;
use bookdata::prelude::*;
use bookdata::gender::*;
use bookdata::arrow::*;
use bookdata::arrow::fusion::*;
use bookdata::arrow::row_de::{scan_parquet_file, RecordBatchDeserializer};

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
  let mut iter = scan_parquet_file("viaf/author-name-index.parquet")?;
  while let Some(row) = iter.next()? {
    let row: NameRow = row;
    // map.entry(row.rec_id).or_default().push(row.name);
  }

  Ok(map)
}

/// Load VIAF author genders
fn viaf_load_genders() -> Result<HashMap<u32, HashSet<Gender>>> {
  let mut map: HashMap<u32, HashSet<Gender>> = HashMap::new();

  info!("loading VIAF author genders");
  let mut iter = scan_parquet_file("viaf/author-genders.parquet")?;
  while let Some(row) = iter.next()? {
    let row: GenderRow = row;
    let gender: Gender = row.gender.into();
    map.entry(row.rec_id).or_default().insert(gender);
  }

  Ok(map)
}

/// Load the VIAF author gender records.
fn viaf_author_gender_records_df(ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
  info!("loading VIAF author names");
  let names = ctx.read_parquet("viaf/author-name-index.parquet")?;
  info!("loading VIAF author genders");
  let genders = ctx.read_parquet("viaf/author-genders.parquet")?;
  let linked = names.join(genders, JoinType::Left, &["rec_id"], &["rec_id"])?;
  // let sorted = linked.sort(vec![col("name").sort(true, true)])?;
  Ok(linked)
}

/// Load the VIAF author gender records.
pub fn viaf_author_table() -> Result<AuthorTable> {
  let mut table = AuthorTable::new();

  let names = viaf_load_names()?;
  let genders = viaf_load_genders()?;

  info!("read {} gender records", table.len());

  Ok(table)
}
