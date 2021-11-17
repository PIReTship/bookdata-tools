use std::str::FromStr;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

use bookdata::prelude::*;
use bookdata::io::LineProcessor;

mod common;
mod author;
mod work;
mod edition;

use common::*;

#[derive(Debug)]
enum DataType {
  Work,
  Edition,
  Author
}

impl FromStr for DataType {
  type Err = Error;

  fn from_str(s: &str) -> Result<DataType> {
    match s {
      "work" => Ok(DataType::Work),
      "edition" => Ok(DataType::Edition),
      "author" => Ok(DataType::Author),
      s => Err(anyhow!("invalid data type {}", s))
    }
  }
}

/// Scan OpenLibrary data.
#[derive(StructOpt, Debug)]
#[structopt(name="scan-openlib")]
struct ScanOpenlib {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Specify type of OpenLibrary data to parse.
  #[structopt(short="m", long="mode")]
  mode: DataType,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

fn scan_openlib<P, R, Proc>(path: P, proc: Proc) -> Result<()>
where P: AsRef<Path>, Proc: OLProcessor<R>, R: DeserializeOwned
{
  let mut proc = proc;
  let mut nlines = 0;
  info!("opening file {}", path.as_ref().to_string_lossy());
  let input = LineProcessor::open_gzip(path)?;

  for line in input.records() {
    nlines += 1;
    if !line.is_ok() {
      error!("parse error on line {}", nlines);
    }
    let row: Row<R> = line?;
    proc.process_row(row)?;
  }

  proc.finish()?;

  Ok(())
}

fn main() -> Result<()> {
  let options = ScanOpenlib::from_args();
  options.common.init()?;

  match options.mode {
    DataType::Author => {
      scan_openlib(options.infile, author::Processor::new()?)?;
    },
    DataType::Work => {
      scan_openlib(options.infile, work::Processor::new()?)?;
    },
    DataType::Edition => {
      scan_openlib(options.infile, edition::Processor::new()?)?;
    }
  };

  Ok(())
}
