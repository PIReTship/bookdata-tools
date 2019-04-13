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
use commands::*;

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
  pcat(pcat::Options),
  /// Make a UUID
  #[structopt(name="make-uuid")]
  make_uuid(make_uuid::Options),
  /// Parse a MARC XML file
  #[structopt(name="parse-marc")]
  parse_marc(parse_marc::Options),
  /// Import JSON data
  #[structopt(name="import-json")]
  import_json(import_json::Options)
}

fn main() -> Result<()> {
  let opt = Opt::from_args();
  opt.logging.init()?;
  match opt.command {
    Command::pcat(opts) => pcat::exec(opts),
    Command::make_uuid(opts) => make_uuid::exec(opts),
    Command::parse_marc(opts) => parse_marc::exec(opts),
    Command::import_json(opts) => import_json::exec(opts)
  }
}
