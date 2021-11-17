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

use structopt::StructOpt;

use serde::{Serialize, Deserialize};

use bookdata::prelude::*;
use bookdata::gender::*;
use bookdata::arrow::*;
use bookdata::ids::codes::*;

mod authors;
mod clusters;

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
  author_file: PathBuf,
}

/// Record format for saving gender information.
#[derive(Serialize, Deserialize, Clone, TableRow)]
struct ClusterGenderInfo {
  cluster: i32,
  gender: String,
}

fn save_genders(clusters: Vec<i32>, genders: clusters::ClusterTable, outf: &Path) -> Result<()> {
  info!("writing cluster genders to {}", outf.display());
  let mut out = TableWriter::open(outf)?;

  for cluster in clusters {
    let mut gender = "no-book-author".to_owned();
    if NS_ISBN.from_code(cluster).is_some() {
      gender = "no-book".to_owned();
    }
    if let Some(stats) = genders.get(&cluster) {
      if stats.n_book_authors == 0 {
        assert!(stats.genders.is_empty());
        gender = "no-book-author".to_owned() // shouldn't happen but 🤷‍♀️
      } else if stats.n_author_recs == 0 {
        assert!(stats.genders.is_empty());
        gender = "no-author-rec".to_owned()
      } else if stats.genders.is_empty() {
        gender = "no-gender".to_owned()
      } else {
        gender = resolve_gender(&stats.genders).to_string()
      };
    }
    out.write_object(ClusterGenderInfo {
      cluster, gender
    })?;
  }

  out.finish()?;

  Ok(())
}

fn main() -> Result<()> {
  let opts = CLI::from_args();
  opts.common.init()?;

  let clusters = clusters::all_clusters("book-links/cluster-stats.parquet")?;
  let name_genders = authors::viaf_author_table()?;
  let cluster_genders = clusters::read_resolve(opts.author_file, &name_genders)?;
  save_genders(clusters, cluster_genders, opts.output.as_ref())?;

  Ok(())
}
