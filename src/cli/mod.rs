//! Command line interface for book data.
//!
//! The book data tools are implemented as a single monolithic executable (to reduce
//! compilation time and disk space in common configurations, with different tools
//! implemented as subcommands.  Each subcommand implements the [Command] trait, which
//! exposes the command line arguments and invocation.
mod support;
mod fusion;

pub use support::{CmdEntry, Command};

pub fn commands() -> Vec<Box<dyn CmdEntry>> {
  vec![
    fusion::Fusion::entry(),
  ]
}


use structopt::StructOpt;
use happylog::args::LogOpts;
use anyhow::Result;

#[derive(StructOpt, Debug)]
pub struct CommonOpts {
  #[structopt(flatten)]
  logging: LogOpts
}

impl CommonOpts {
  pub fn init(&self) -> Result<()> {
    self.logging.init()?;
    Ok(())
  }
}
