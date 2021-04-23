use std::io::prelude::*;
use std::io::{BufReader};
use std::fs::{File};
use std::sync::Arc;
use std::path::PathBuf;
use std::mem::drop;

use log::*;

use structopt::StructOpt;
use flate2::bufread::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use anyhow::{Result};
use serde::{Deserialize};
use serde_json::from_str;
use happylog::set_progress;
use arrow::datatypes::*;
use arrow::array::*;

use crate::parquet::*;
use crate::index::IdIndex;
use crate::commands::Command;

/// Scan GoodReads interaction file into Parquet
#[derive(StructOpt)]
#[structopt(name="gr-scan-interactions")]
pub struct ScanInteractions {
  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf,

  /// Ouptut file
  #[structopt(name = "OUTPUT", parse(from_os_str))]
  outfile: PathBuf
}

#[derive(Deserialize)]
struct RawInteraction {
  user_id: String,
  book_id: String,
  // review_id: String,
  #[serde(rename="isRead")]
  is_read: bool,
  rating: i32,
  // date_added: String
}

struct IntRecord {
  rec_id: u64,
  user_id: u64,
  book_id: u64,
  is_read: bool,
  rating: Option<i8>,
}

struct GRIBatch {
  rec_id: UInt64Builder,
  user_id: UInt64Builder,
  book_id: UInt64Builder,
  is_read: UInt8Builder,
  rating: Int8Builder,
}

impl TableRow for IntRecord {
  type Batch = GRIBatch;

  fn schema() -> Schema {
    Schema::new(vec![
      Field::new("rec_id", DataType::UInt64, false),
      Field::new("user_id", DataType::UInt64, false),
      Field::new("book_id", DataType::UInt64, false),
      Field::new("is_read", DataType::UInt8, false),
      Field::new("rating", DataType::Int8, true),
    ])
  }

  fn new_batch(cap: usize) -> GRIBatch {
    GRIBatch {
      rec_id: UInt64Builder::new(cap),
      user_id: UInt64Builder::new(cap),
      book_id: UInt64Builder::new(cap),
      is_read: UInt8Builder::new(cap),
      rating: Int8Builder::new(cap),
    }
  }

  fn finish_batch(batch: &mut GRIBatch) -> Vec<ArrayRef> {
    vec![
      Arc::new(batch.rec_id.finish()),
      Arc::new(batch.user_id.finish()),
      Arc::new(batch.book_id.finish()),
      Arc::new(batch.is_read.finish()),
      Arc::new(batch.rating.finish()),
    ]
  }

  fn write_to_batch(&self, batch: &mut GRIBatch) -> Result<()> {
    batch.rec_id.append_value(self.rec_id)?;
    batch.user_id.append_value(self.user_id)?;
    batch.book_id.append_value(self.book_id)?;
    batch.is_read.append_value(self.is_read as u8)?;
    batch.rating.append_option(self.rating)?;
    Ok(())
  }
}

impl Command for ScanInteractions {
  fn exec(self) -> Result<()> {
    let infn = &self.infile;
    let outfn = &self.outfile;
    info!("reading interactions from {:?}", infn);
    let fs = File::open(infn)?;
    let pb = ProgressBar::new(fs.metadata()?.len());
    pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})"));
    let _pbl = set_progress(&pb);
    // And wrap it in progress and decompression
    let pbr = pb.wrap_read(fs);
    let pbr = BufReader::new(pbr);
    let gzf = MultiGzDecoder::new(pbr);
    let bfs = BufReader::new(gzf);

    info!("writing interactions to {:?}", outfn);
    let mut users = IdIndex::new();
    let mut writer = TableWriter::open(outfn)?;
    let mut n_recs = 0;

    for line in bfs.lines() {
      let line = line?;
      let row: RawInteraction = from_str(&line)?;
      let rec_id = n_recs + 1;
      n_recs += 1;
      let key = hex::decode(row.user_id.as_bytes())?;
      let user_id = users.intern(key);
      let book_id: u64 = row.book_id.parse()?;
      let record = IntRecord {
        rec_id, user_id, book_id,
        is_read: row.is_read,
        rating: if row.rating > 0 {
          Some(row.rating as i8)
        } else {
          None
        }
      };
      writer.write(&record)?;
    }
    pb.finish_and_clear();
    drop(_pbl);

    let nlines = writer.finish()?;

    info!("wrote {} records", nlines);

    Ok(())
  }
}
