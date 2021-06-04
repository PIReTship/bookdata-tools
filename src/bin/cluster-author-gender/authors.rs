//! Support for loading author info.
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::collections::{HashMap,HashSet};
use std::sync::Arc;

use structopt::StructOpt;
use futures::{StreamExt};

use happylog::set_progress;

use serde::{Serialize, Deserialize};

use tokio;

use arrow::datatypes::*;
use datafusion::prelude::*;
use datafusion::physical_plan::ExecutionPlan;
use parquet::record::reader::RowIter;
use parquet::record::RowAccessor;
use bookdata::prelude::*;
use bookdata::io::progress::*;
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
  let start = Instant::now();

  info!("loading VIAF author names");
  let file = open_parquet_file("viaf/author-name-index.parquet")?;

  // verify the file schema
  let meta = file.metadata().file_metadata();
  let schema = meta.schema_descr();
  assert_eq!(schema.num_columns(), 2);
  assert_eq!(schema.column(0).name(), "rec_id");
  assert_eq!(schema.column(1).name(), "name");

  let iter = RowIter::from_file(None, &file)?;
  for row in iter {
    let rec_id = row.get_uint(0)?;
    let name = row.get_string(1)?;
    map.entry(rec_id).or_default().push(name.clone());
  }

  info!("loaded authors for {} records in {}", map.len(), human_time(start.elapsed()));

  Ok(map)
}

/// Load VIAF author genders
fn viaf_load_genders() -> Result<HashMap<u32, HashSet<Gender>>> {
  let mut map: HashMap<u32, HashSet<Gender>> = HashMap::new();
  let start = Instant::now();

  info!("loading VIAF author genders");
  let mut iter = scan_parquet_file("viaf/author-genders.parquet")?;
  while let Some(row) = iter.next()? {
    let row: GenderRow = row;
    let gender: Gender = row.gender.into();
    map.entry(row.rec_id).or_default().insert(gender);
  }

  info!("loaded genders for {} records in {}", map.len(), human_time(start.elapsed()));

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
