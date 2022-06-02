//! Index names from authority records.
use std::cmp::Reverse;
use std::collections::{HashSet, HashMap, BinaryHeap};
use std::path::{PathBuf, Path};
use std::fs::File;

use structopt::StructOpt;
use csv;
use serde::{Deserialize, Serialize};
use flate2::write::GzEncoder;
use zstd::stream::Encoder;

use crate::prelude::*;
use crate::arrow::*;
use crate::io::object::ThreadWriter;
use crate::cleaning::names::*;
use crate::io::open_gzin_progress;

#[derive(StructOpt, Debug)]
#[structopt(name="index-names")]
/// Clean and index author names from authority records.
pub struct IndexNames {
  /// Name input CSV file.
  #[structopt(name = "INFILE", parse(from_os_str))]
  infile: PathBuf,

  /// Index output Parquet file.
  #[structopt(name = "OUTFILE", parse(from_os_str))]
  outfile: PathBuf,
}

type NameIndex = HashMap<String,HashSet<u32>>;

#[derive(Deserialize)]
struct RecAuthor {
  rec_id: u32,
  #[allow(dead_code)]
  ind: Option<char>,
  name: String,
}

#[derive(ParquetRecordWriter, Serialize, Clone)]
struct IndexEntry {
  rec_id: u32,
  name: String,
}

fn scan_names(path: &Path) -> Result<NameIndex> {
  info!("reading names from {}", path.to_string_lossy());
  let mut index = NameIndex::new();
  let reader = open_gzin_progress(path)?;
  let mut reader = csv::Reader::from_reader(reader);
  for line in reader.deserialize() {
    let record: RecAuthor = line?;
    for name in name_variants(&record.name)? {
      index.entry(name).or_default().insert(record.rec_id);
    }
  }
  Ok(index)
}

fn write_index(index: NameIndex, path: &Path) -> Result<()> {
  info!("sorting {} names", index.len());
  let mut names = BinaryHeap::with_capacity(index.len());
  names.extend(index.keys().map(|k| Reverse(k)));

  info!("writing deduplicated names to {}", path.to_string_lossy());
  let mut writer = TableWriter::open(&path)?;

  let mut csv_fn = PathBuf::from(path);
  csv_fn.set_extension("csv.gz");
  let out = File::create(&csv_fn)?;
  let out = GzEncoder::new(out, flate2::Compression::fast());
  // let out = Encoder::new(out, 2)?.auto_finish();
  let csvw = csv::Writer::from_writer(out);
  let mut csvout = ThreadWriter::new(csvw);

  while let Some(Reverse(name)) = names.pop() {
    let mut ids: Vec<u32> = index.get(name).unwrap().iter().map(|i| *i).collect();
    ids.sort();
    for rec_id in ids {
      let e = IndexEntry {
        rec_id,
        name: name.to_string()
      };
      csvout.write_object(e.clone())?;
      writer.write_object(e)?;
    }
  }

  writer.finish()?;
  csvout.finish()?;
  Ok(())
}

impl Command for IndexNames {
  fn exec(&self) -> Result<()> {
    let names = scan_names(&self.infile)?;
    write_index(names, &self.outfile)?;

    Ok(())
  }
}
