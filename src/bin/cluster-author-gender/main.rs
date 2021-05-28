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

mod authors;

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

  let authors = authors::viaf_author_table(&mut ctx).await?;

  // let gdf = cluster_gender_records(&mut ctx, opts.author_file.as_str())?;
  // let plan = plan_df(&mut ctx, gdf)?;
  // let clusters = scan_cluster_genders(&mut ctx, plan).await?;

  // save_genders(clusters, opts.output.as_ref())?;

  Ok(())
}
