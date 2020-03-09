mod ops;
mod openlib;
mod goodreads;

use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;

use log::*;

use structopt::StructOpt;
use flate2::bufread::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use sha1::Sha1;
use anyhow::{Result, anyhow};

use crate::io::{HashRead, HashWrite};
use crate::db::{DbOpts, CopyRequest};
use crate::tracking::StageOpts;
use super::Command;

/// Data set definition type - anything implementing DataSetOps
type DataSet = Box<dyn ops::DataSetOps>;

// Parse a data set definition from a string
impl FromStr for DataSet {
  type Err = anyhow::Error;
  fn from_str(s: &str) -> Result<DataSet> {
    match s {
      "openlib" => Ok(Box::new(openlib::Ops {})),
      "goodreads" => Ok(Box::new(goodreads::Ops {})),
      _ => Err(anyhow!("invalid string {}", s))
    }
  }
}

/// Process OpenLib data into format suitable for PostgreSQL import.
#[derive(StructOpt)]
#[structopt(name="import-json")]
pub struct ImportJson {
  #[structopt(flatten)]
  db: DbOpts,

  #[structopt(flatten)]
  stage: StageOpts,

  /// Truncate the table before importing
  #[structopt(long="truncate")]
  truncate: bool,

  /// Specify the type of dataset to work on
  #[structopt()]
  dataset: DataSet,

  /// Specify the table name into which to import
  #[structopt(name = "TABLE")]
  table: String,
  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

impl Command for ImportJson {
  fn exec(self) -> Result<()> {
    let dbo = self.db.default_schema(self.dataset.schema());

    let dbc = dbo.open()?;
    self.stage.begin_stage(&dbc)?;

    // Set up the input file, tracking read progress
    let infn = &self.infile;
    info!("reading from {:?}", infn);
    let fs = File::open(infn)?;
    let pb = ProgressBar::new(fs.metadata()?.len());
    pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})"));

    // We want to hash the file while we read it
    let mut in_hash = Sha1::new();
    let read = HashRead::create(fs, &mut in_hash);
    // And wrap it in progress
    let pbr = pb.wrap_read(read);
    let pbr = BufReader::new(pbr);
    let gzf = MultiGzDecoder::new(pbr);
    let mut bfs = BufReader::new(gzf);

    // Set up the output stream, writing to the database
    let req = CopyRequest::new(&dbo, &self.dataset.table_name(&self.table))?;
    let req = req.with_schema(dbo.schema());
    let columns = self.dataset.columns(&self.table);
    let cref: Vec<&str> = columns.iter().map(String::as_str).collect();
    let req = req.with_columns(&cref);
    let req = req.truncate(self.truncate);
    let out = req.open()?;
    let mut out_hash = Sha1::new();
    let hout = HashWrite::create(out, &mut out_hash);
    let mut buf_out = BufWriter::new(hout);

    // Actually run the import
    let n = self.dataset.import(&mut bfs, &mut buf_out)?;
    buf_out.flush()?;
    drop(buf_out);

    // Grab the hashes and save them to the transcript
    let in_hash = in_hash.hexdigest();
    let out_hash = out_hash.hexdigest();
    let mut t_out = self.stage.open_transcript()?;
    info!("loaded {} records with hash {}", n, out_hash);
    writeln!(&mut t_out, "SOURCE {:?}", infn)?;
    writeln!(&mut t_out, "SHASH {}", in_hash)?;
    writeln!(&mut t_out, "HASH {}", out_hash)?;

    // All done! Record success and exit.
    self.stage.record_file(&dbc, infn, &in_hash)?;
    self.stage.end_stage(&dbc, &Some(out_hash))?;
    Ok(())
  }
}
