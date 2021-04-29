use std::path::PathBuf;
use std::time::Instant;

use log::*;

use glob::glob;
use structopt::StructOpt;
use anyhow::{Result};
use fallible_iterator::FallibleIterator;
use happylog::set_progress;

use bookdata::prelude::*;
use bookdata::io::open_gzin_progress;
use bookdata::parquet::*;
use bookdata::marc::parse::{read_records, read_records_delim};
use bookdata::marc::flat_fields::Output;

/// Parse MARC files into Parquet tables.
#[derive(StructOpt, Debug)]
#[structopt(name="scan-marc")]
pub struct ParseMarc {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Output file
  #[structopt(short="o", long="output")]
  output: PathBuf,

  /// Activate line mode, e.g. for VIAF
  #[structopt(short="L", long="line-mode")]
  linemode: bool,

  #[structopt(long="src-dir")]
  src_dir: Option<PathBuf>,

  #[structopt(long="src-prefix")]
  src_prefix: Option<String>,

  /// Input files to parse (GZ-compressed)
  #[structopt(name = "FILE", parse(from_os_str))]
  files: Vec<PathBuf>
}

fn main() -> Result<()> {
  let opts = ParseMarc::from_args();
  opts.common.init()?;

  info!("preparing to write {:?}", &opts.output);
  let writer = TableWriter::open(&opts.output)?;
  let mut writer = Output::new(writer);
  let mut nfiles = 0;
  let all_start = Instant::now();

  for inf in opts.find_files()? {
    nfiles += 1;
    let inf = inf.as_path();
    let file_start = Instant::now();
    info!("reading from compressed file {:?}", inf);
    let (read, pb) = open_gzin_progress(inf)?;
    let _pbl = set_progress(&pb);
    let mut records = if opts.linemode {
      read_records_delim(read)
    } else {
      read_records(read)
    };

    let mut nrecs = 0;
    while let Some(rec) = records.next()? {
      writer.write_marc_record(rec)?;
      nrecs += 1;
    }

    pb.finish_and_clear();
    info!("processed {} records from {:?} in {:.2}s",
          nrecs, inf, file_start.elapsed().as_secs_f32());
  }

  let nrecs = writer.finish()?;

  info!("imported {} records from {} files in {:.2}s",
        nrecs, nfiles, all_start.elapsed().as_secs_f32());

  Ok(())
}

impl ParseMarc {
  fn find_files(&self) -> Result<Vec<PathBuf>> {
    if let Some(ref dir) = self.src_dir {
      let mut ds = dir.to_str().unwrap().to_string();
      if let Some(ref pfx) = self.src_prefix {
        ds.push_str("/");
        ds.push_str(pfx);
        ds.push_str("*.xml.gz");
      } else {
        ds.push_str("/*.xml.gz");
      }
      info!("scanning for files {}", ds);
      let mut v = Vec::new();
      for entry in glob(&ds)? {
        let entry = entry?;
        v.push(entry);
      }
      Ok(v)
    } else {
      Ok(self.files.clone())
    }
  }
}
