mod error;
mod cleaning;
mod tsv;
mod db;
mod logging;
mod commands;

use structopt::StructOpt;

use std::io;
use std::fs::File;
use std::path::PathBuf;
use indicatif::{ProgressBar, ProgressStyle};

use error::Result;
use logging::LogOpts;
use commands::pcat;

/// BookData import tools
#[derive(StructOpt, Debug)]
#[structopt(name="bookdata")]
struct Opt {
  #[structopt(flatten)]
  logging: LogOpts,

  #[structopt(subcommand)]
  command: Command
}

#[derive(StructOpt, Debug)]
#[allow(non_camel_case_types)]
enum Command {
  /// Output a file with a progress bar
  pcat(pcat::Options)
}

fn main() -> Result<()> {
  let opt = Opt::from_args();
  opt.logging.init()?;
  match opt.command {
    Command::pcat(opts) => pcat::exec(opts)
  }
}
