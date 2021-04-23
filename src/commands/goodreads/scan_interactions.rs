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
  review_id: String,
  #[serde(rename="isRead")]
  is_read: bool,
  rating: i32,
  date_added: String
}

struct GRIBatch {
  rec_id: UInt64Builder,
  user_id: UInt64Builder,
  book_id: UInt64Builder,
  is_read: UInt8Builder,
  rating: Int8Builder,
}

struct GRIWrite {
  n_recs: u64,
  user_ids: IdIndex<Vec<u8>>,
}

impl GRIWrite {
  fn new() -> GRIWrite {
    GRIWrite {
      n_recs: 0,
      user_ids: IdIndex::new()
    }
  }
}

impl TableWrite for GRIWrite {
  type Batch = GRIBatch;
  type Record = RawInteraction;

  fn new_builder(cap: usize) -> GRIBatch {
    GRIBatch {
      rec_id: UInt64Builder::new(cap),
      user_id: UInt64Builder::new(cap),
      book_id: UInt64Builder::new(cap),
      is_read: UInt8Builder::new(cap),
      rating: Int8Builder::new(cap),
    }
  }

  fn finish(&self, batch: &mut GRIBatch) -> Vec<ArrayRef> {
    vec![
      Arc::new(batch.rec_id.finish()),
      Arc::new(batch.user_id.finish()),
      Arc::new(batch.book_id.finish()),
      Arc::new(batch.is_read.finish()),
      Arc::new(batch.rating.finish()),
    ]
  }

  fn schema() -> Schema {
    Schema::new(vec![
      Field::new("rec_id", DataType::UInt64, false),
      Field::new("user_id", DataType::UInt64, false),
      Field::new("book_id", DataType::UInt64, false),
      Field::new("is_read", DataType::Boolean, false),
      Field::new("rating", DataType::Int8, true),
    ])
  }

  fn write(&mut self, row: &RawInteraction, batch: &mut GRIBatch) -> Result<()> {
    let rid = self.n_recs + 1;
    self.n_recs += 1;
    let key = hex::decode(row.user_id.as_bytes())?;
    let uid = self.user_ids.intern(key);
    let bid: u64 = row.book_id.parse()?;
    batch.rec_id.append_value(rid)?;
    batch.user_id.append_value(uid)?;
    batch.book_id.append_value(bid)?;
    batch.is_read.append_value(row.is_read as u8)?;
    if row.rating > 0 {
      batch.rating.append_value(row.rating as i8)?;
    } else {
      batch.rating.append_null()?;
    }
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
    let mut tw = GRIWrite::new();
    let mut writer = TableWriter::open(outfn, &mut tw)?;

    for line in bfs.lines() {
      let line = line?;
      let raw: RawInteraction = from_str(&line)?;
      writer.write(&raw)?;
    }
    pb.finish_and_clear();
    drop(_pbl);

    let nlines = writer.finish()?;

    info!("wrote {} records", nlines);

    Ok(())
  }
}
