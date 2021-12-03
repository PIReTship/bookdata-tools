//! Index names from authority records.
use std::collections::{HashSet, HashMap};
use std::path::{PathBuf, Path};
use std::fs::File;

use structopt::StructOpt;
use csv;
use serde::{Deserialize, Serialize};
use flate2::write::GzEncoder;

use happylog::set_progress;

use crate::prelude::*;
use crate::arrow::*;
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

fn scan_names<P: AsRef<Path>>(path: P) -> Result<NameIndex> {
  info!("reading names from {}", path.as_ref().to_string_lossy());
  let mut index = NameIndex::new();
  let (reader, pb) = open_gzin_progress(path)?;
  let _pbl = set_progress(&pb);
  let mut reader = csv::Reader::from_reader(reader);
  for line in reader.deserialize() {
    let record: RecAuthor = line?;
    for name in name_variants(&record.name)? {
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
  let mut writer = TableWriter::open(&path)?;
  let mut csv_fn = PathBuf::from(path.as_ref());
  csv_fn.set_extension("csv.gz");
  let out = File::create(&csv_fn)?;
  let out = GzEncoder::new(out, flate2::Compression::best());
  let mut csvw = csv::Writer::from_writer(out);
  for name in names {
    for rec_id in index.get(name).unwrap() {
      let e = IndexEntry {
        rec_id: *rec_id,
        name: name.to_string()
      };
      csvw.serialize(&e)?;
      writer.write_object(e)?;
    }
  }

  writer.finish()?;
  Ok(())
}

impl Command for IndexNames {
  fn exec(&self) -> Result<()> {
    let names = scan_names(&self.infile)?;
    write_index(names, &self.outfile)?;

    Ok(())
  }
}
