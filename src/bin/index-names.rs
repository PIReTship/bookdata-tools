use std::collections::{HashSet, HashMap};
use std::path::{PathBuf, Path};

use structopt::StructOpt;
use csv;
use serde::{Deserialize};

use happylog::set_progress;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::cleaning::names::*;
use bookdata::io::open_gzin_progress;

#[derive(StructOpt, Debug)]
#[structopt(name="index-names")]
/// Clean and index author names.
pub struct IndexNames {
  #[structopt(flatten)]
  common: CommonOpts,

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
  ind: Option<u8>,
  name: String,
}

#[derive(TableRow)]
struct IndexEntry {
  rec_id: u32,
  name: String,
}

fn scan_names<P: AsRef<Path>>(path: P) -> Result<NameIndex> {
  info!("reading names from {}", path.as_ref().to_string_lossy());
  let mut index = NameIndex::new();
  let (reader, pb) = open_gzin_progress(path)?;
  let _pbl = set_progress(&pb);
  let mut reader = csv::Reader::from_reader(reader);
  for line in reader.deserialize() {
    let record: RecAuthor = line?;
    for name in name_variants(&record.name) {
      index.entry(name).or_default().insert(record.rec_id);
    }
  }
  Ok(index)
}

fn write_index<P: AsRef<Path>>(index: NameIndex, path: P) -> Result<()> {
  let mut names: Vec<&str> = index.keys().map(|s| s.as_str()).collect();
  info!("sorting {} names", names.len());
  names.sort();
  info!("writing deduplicated names to {}", path.as_ref().to_string_lossy());
  let mut writer = TableWriter::open(path)?;
  for name in names {
    for rec_id in index.get(name).unwrap() {
      writer.write_object(IndexEntry {
        rec_id: *rec_id,
        name: name.to_string()
      })?;
    }
  }

  writer.finish()?;
  Ok(())
}

fn main() -> Result<()> {
  let opts = IndexNames::from_args();
  opts.common.init()?;

  let names = scan_names(&opts.infile)?;
  write_index(names, opts.outfile)?;

  Ok(())
}
