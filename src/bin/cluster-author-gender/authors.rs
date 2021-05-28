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
use bookdata::arrow::row_de::RecordBatchDeserializer;

#[derive(Debug, Default)]
pub struct AuthorInfo {
  n_author_recs: u32,
  genders: HashSet<Gender>
}

pub type AuthorTable = HashMap<String,AuthorInfo>;

#[derive(Deserialize, Debug)]
struct AuthorRow {
  name: String,
  rec_id: Option<u64>,
  gender: Option<String>
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
pub async fn viaf_author_table(ctx: &mut ExecutionContext) -> Result<AuthorTable> {
  let df = viaf_author_gender_records_df(ctx)?;
  let plan = plan_df(ctx, df)?;

  let mut table = AuthorTable::new();

  info!("scanning author table");
  let stream = plan.execute(0).await?;
  let mut rec_stream = RecordBatchDeserializer::for_stream(stream);
  while let Some(row) = rec_stream.next().await {
    let row: AuthorRow = row?;
    let mut e = table.entry(row.name).or_default();
    if let Some(_) = row.rec_id {
      e.n_author_recs += 1;
    }
    if let Some(g) = row.gender {
      e.genders.insert(g.into());
    }
  }

  info!("read {} gender records", table.len());

  Ok(table)
}
