use std::path::{Path, PathBuf};
use std::collections::HashMap;

use serde::Deserialize;
use chrono::prelude::*;
use friendly;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::ids::codes::*;
use bookdata::ids::index::IdIndex;
use bookdata::io::object::ThreadWriter;
use bookdata::parsing::*;
use bookdata::parsing::dates::*;
use anyhow::Result;

const OUT_FILE: &'static str = "gr-interactions.parquet";
const LINK_FILE: &'static str = "gr-book-link.parquet";

/// Scan GoodReads interaction file into Parquet
#[derive(StructOpt)]
#[structopt(name="scan-interactions")]
pub struct ScanInteractions {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

// the records we read from JSON
#[derive(Deserialize)]
struct RawInteraction {
  user_id: String,
  book_id: String,
  // review_id: String,
  #[serde(rename="isRead")]
  is_read: bool,
  rating: f32,
  date_added: String,
  date_updated: String,
  read_at: String,
  started_at: String,
}

// the records we're actually going to write to the table
#[derive(TableRow)]
struct IntRecord {
  rec_id: u32,
  user_id: u32,
  book_id: i32,
  cluster: Option<i32>,
  is_read: u8,
  rating: Option<f32>,
  added: DateTime<FixedOffset>,
  updated: DateTime<FixedOffset>,
  read_started: Option<DateTime<FixedOffset>>,
  read_finished: Option<DateTime<FixedOffset>>,
}

// Object writer to transform and write GoodReads interactions
struct IntWriter {
  writer: TableWriter<IntRecord>,
  clusters: HashMap<i32,i32>,
  users: IdIndex<Vec<u8>>,
  n_recs: u32,
}

impl IntWriter {
  // Open a new output
  pub fn open<P: AsRef<Path>>(path: P) -> Result<IntWriter> {
    let clusters = load_cluster_map()?;
    let writer = TableWriter::open(path)?;
    Ok(IntWriter {
      writer, clusters,
      users: IdIndex::new(),
      n_recs: 0
    })
  }
}

impl ObjectWriter<RawInteraction> for IntWriter {
  // Write a single interaction to the output
  fn write_object(&mut self, row: RawInteraction) -> Result<()> {
    self.n_recs += 1;
    let rec_id = self.n_recs;
    let user_key = hex::decode(row.user_id.as_bytes())?;
    let user_id = self.users.intern_owned(user_key);
    let book_id: i32 = row.book_id.parse()?;
    let cluster = self.clusters.get(&book_id).map(|c| *c);
    if !cluster.is_some() {
      warn!("book {} has no cluster", book_id);
    }

    self.writer.write_object(IntRecord {
      rec_id, user_id, book_id, cluster,
      is_read: row.is_read as u8,
      rating: if row.rating > 0.0 {
        Some(row.rating)
      } else {
        None
      },
      added: parse_gr_date(&row.date_added)?,
      updated: parse_gr_date(&row.date_updated)?,
      read_started: trim_opt(&row.started_at).map(parse_gr_date).transpose()?,
      read_finished: trim_opt(&row.read_at).map(parse_gr_date).transpose()?,
    })?;

    Ok(())
  }

  // Clean up and finalize output
  fn finish(self) -> Result<usize> {
    info!("wrote {} records for {} users, closing output", self.n_recs, self.users.len());
    self.writer.finish()
  }
}

#[derive(Deserialize)]
struct IdCluster {
  book_id: i32,
  cluster: Option<i32>
}

/// Load mapping from book IDs to clusters.
fn load_cluster_map() -> Result<HashMap<i32, i32>> {
  info!("loading book cluster map");
  let mut map = HashMap::new();
  let mut read = scan_parquet_file::<IdCluster, _>(LINK_FILE)?;
  while let Some(rec) = read.next()? {
    if let Some(c) = rec.cluster {
      map.insert(rec.book_id, c);
    }
  }
  info!("loaded clusters for {} book IDs", map.len());
  Ok(map)
}

fn main() -> Result<()> {
  let options = ScanInteractions::from_args();
  options.common.init()?;

  let writer = IntWriter::open(OUT_FILE)?;

  info!("reading interactions from {}", &options.infile.display());
  let proc = LineProcessor::open_gzip(&options.infile)?;

  let mut writer = ThreadWriter::new(writer);
  proc.process_json(&mut writer)?;
  writer.finish()?;

  info!("output {} is {}", OUT_FILE, friendly::bytes(file_size(OUT_FILE)?));

  Ok(())
}
