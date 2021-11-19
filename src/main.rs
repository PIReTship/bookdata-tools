//! Code for processing and integrating book data.
//!
//! The `bookdata` crate (not published) provides the support and utility
//! code for the various programs used to integrate the book data.  If you
//! are writing additional integrations or analyses, you may find the
//! modules and functions in here useful.
use anyhow::Result;
use structopt::StructOpt;

use happylog::args::LogOpts;
use bookdata::cli::{Command, BDCommand};

/// BookData import tools
#[derive(StructOpt, Debug)]
#[structopt(name="bookdata")]
struct Opt {
  #[structopt(flatten)]
  logging: LogOpts,

  #[structopt(subcommand)]
  command: BDCommand,
}

fn main() -> Result<()> {
  let opt = Opt::from_args();
  opt.logging.init()?;
  opt.command.exec()
}
