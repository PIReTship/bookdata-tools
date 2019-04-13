mod error;
mod cleaning;
mod tsv;
mod db;
mod logging;

use structopt::StructOpt;

use std::io;
use std::fs::File;
use std::path::PathBuf;
use indicatif::{ProgressBar, ProgressStyle};

use error::Result;
use logging::LogOpts;

/// BookData import tools
#[derive(StructOpt, Debug)]
#[structopt(name="bookdata")]
struct Opt {
  #[structopt(flatten)]
  logging: LogOpts
}

fn main() -> Result<()> {
  let opt = Opt::from_args();
  opt.logging.init()?;
  Ok(())
}
