use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

use crate::prelude::*;
use crate::io::LineProcessor;
use crate::openlib::*;

use super::Command;

#[derive(StructOpt, Debug)]
struct Input {
  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

#[derive(StructOpt, Debug)]
enum DataType {
  /// Parse OpenLibrary works.
  ///
  /// Authors must be processed first.
  Work(Input),

  /// Parse OpenLibrary editions.
  ///
  /// Authors and works must be processed first.
  Edition(Input),

  /// Parse OpenLibrary authors.
  Author(Input)
}

/// Scan OpenLibrary data.
#[derive(StructOpt, Debug)]
#[structopt(name="openlib")]
pub struct OpenLib {
  #[structopt(flatten)]
  common: CommonOpts,

  #[structopt(subcommand)]
  mode: DataType,
}

/// Helper function to route OpenLibrary data.
fn scan_openlib<P, R, Proc>(path: P, proc: Proc) -> Result<()>
where P: AsRef<Path>, Proc: ObjectWriter<Row<R>>, R: DeserializeOwned
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
    proc.write_object(row)?;
  }

  proc.finish()?;

  Ok(())
}

impl Command for OpenLib {
  fn exec(&self) -> Result<()> {
    match &self.mode {
      DataType::Author(opts) => {
        scan_openlib(&opts.infile, AuthorProcessor::new()?)?;
      },
      DataType::Work(opts) => {
        scan_openlib(&opts.infile, WorkProcessor::new()?)?;
      },
      DataType::Edition(opts) => {
        scan_openlib(&opts.infile, EditionProcessor::new()?)?;
      }
    };

    Ok(())
  }
}
