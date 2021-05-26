//! Summarize author gender information for clusters.
//!
//! This script reads the cluster author information and author gender
//! information, in order to aggregate author genders for each cluster.
//! We do this in Rust code, because DataFusion's aggregate support is
//! not performant for very high-cardinality aggregations.  Therefore
//! we use DataFusion to load and join the data, but perform the final
//! summarization directly in Rust.
//!
//! We use a lot of left joins so that we can compute statistics across
//! the integration pipeline.
use std::path::{Path, PathBuf};
use std::collections::HashMap;
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
use bookdata::arrow::row_de::{RecordBatchDeserializer, BatchRecordIter};

// #[derive(Display, FromStr, Debug)]
// #[display(style="lowercase")]
// enum GenderSource {
//   VIAF,
// }

#[derive(StructOpt, Debug)]
#[structopt(name="cluster-author-genders")]
/// Extract cluster author data from extracted book data.
struct CLI {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Specify output file
  #[structopt(short="o", long="output")]
  output: PathBuf,

  /// Specify the cluster-author file.
  #[structopt(short="A", long="cluster-authors")]
  author_file: String,
}

/// Record format for saving gender information.
#[derive(Serialize, Deserialize, Clone, TableRow)]
struct ClusterGenderInfo {
  cluster: i32,
  gender: String,
}

/// Record format for integration results (pre-aggregate).
#[derive(Deserialize, Debug)]
struct ClusterLinkRecord {
  cluster: i32,
  author_name: Option<String>,
  rec_id: Option<i32>,
  gender: Option<String>,
}

/// Record for storing a cluster's gender statistics while aggregating.
#[derive(Debug, Default)]
struct ClusterStats {
  n_book_authors: u32,
  n_author_recs: u32,
  n_gender_recs: u32,
  genders: HashMap<Gender,u32>
}

/// Load the cluster authors.
fn cluster_author_records(ctx: &mut ExecutionContext, caf: &str) -> Result<Arc<dyn DataFrame>> {
  info!("loading ISBN clusters");
  let icl = ctx.read_parquet("book-links/cluster-stats.parquet")?;
  let icl = icl.select_columns(&["cluster"])?;
  info!("loading author names");
  let can = ctx.read_parquet(caf)?;
  let clust_auth = icl.join(can, JoinType::Left, &["cluster"], &["cluster"])?;
  Ok(clust_auth)
}

/// Load the VIAF author gender records.
fn viaf_author_gender_records(ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
  info!("loading VIAF author names");
  let names = ctx.read_parquet("viaf/author-name-index.parquet")?;
  info!("loading VIAF author genders");
  let schema = Schema::new(vec![
    Field::new("rec_id", DataType::UInt32, false),
    Field::new("gender", DataType::Utf8, false),
  ]);
  let opts = CsvReadOptions::new().schema(&schema).has_header(true);
  let genders = ctx.read_csv("viaf/author-genders.csv.gz", opts)?;
  let linked = names.join(genders, JoinType::Left, &["rec_id"], &["rec_id"])?;
  Ok(linked)
}

/// Load the cluster author gender records
fn cluster_gender_records(ctx: &mut ExecutionContext, caf: &str) -> Result<Arc<dyn DataFrame>> {
  let car = cluster_author_records(ctx, caf)?;
  let agr = viaf_author_gender_records(ctx)?;
  let linked = car.join(agr, JoinType::Left, &["author_name"], &["name"])?;
  Ok(linked)
}

/// Load gender information.
async fn scan_cluster_genders(ctx: &mut ExecutionContext, plan: Arc<dyn ExecutionPlan>) -> Result<HashMap<i32,ClusterStats>> {
  let mut map = HashMap::new();

  info!("scanning record batches");
  let mut stream = plan.execute(0).await?;
  while let Some(batch) = stream.next().await {
    let batch = batch?;
    debug!("received batch");
    let rbd = RecordBatchDeserializer::new(batch)?;
    let mut rows: BatchRecordIter<ClusterLinkRecord> = rbd.iter();
    while let Some(row) = rows.next()? {
      let rec: &mut ClusterStats = map.entry(row.cluster).or_default();
      if row.author_name.is_some() {
        rec.n_book_authors += 1;
      }
      if row.rec_id.is_some() {
        rec.n_author_recs += 1;
      }
      if let Some(g) = row.gender {
        let g: Gender = g.into();
        *rec.genders.entry(g).or_default() += 1;
      }
    }
  }

  info!("scanned {} clusters", map.len());

  Ok(map)
}

fn save_genders(clusters: HashMap<i32,ClusterStats>, outf: &Path) -> Result<()> {
  info!("writing cluster genders to {}", outf.to_string_lossy());
  let mut out = TableWriter::open(outf)?;

  for (cluster, stats) in clusters {
    let gender = if stats.n_book_authors == 0 {
      "no-book-author".to_owned()
    } else if stats.n_author_recs == 0 {
      "no-author-rec".to_owned()
    } else if stats.genders.is_empty() {
      "no-gender".to_owned()
    } else {
      resolve_gender(stats.genders.keys()).to_string()
    };
    out.write_object(ClusterGenderInfo {
      cluster, gender
    })?;
  }

  Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
  let opts = CLI::from_args();
  opts.common.init()?;

  let mut ctx = ExecutionContext::new();

  let gdf = cluster_gender_records(&mut ctx, opts.author_file.as_str())?;
  let plan = plan_df(&mut ctx, gdf)?;
  let clusters = scan_cluster_genders(&mut ctx, plan).await?;

  save_genders(clusters, opts.output.as_ref())?;

  Ok(())
}
