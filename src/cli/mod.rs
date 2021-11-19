//! Command line interface for book data.
//!
//! The book data tools are implemented as a single monolithic executable (to reduce
//! compilation time and disk space in common configurations, with different tools
//! implemented as subcommands.  Each subcommand implements the [Command] trait, which
//! exposes the command line arguments and invocation.
// mod support;

pub mod fusion;
pub mod index_names;
pub mod rdf_scan_nodes;
pub mod rdf_scan_triples;

use structopt::StructOpt;
use enum_dispatch::enum_dispatch;

/// Trait implemented by book data commands.
#[enum_dispatch]
pub trait Command {
  /// Run the command with options
  fn exec(self) -> Result<()>;
}

#[enum_dispatch(Command)]
#[derive(StructOpt, Debug)]
pub enum BDCommand {
  Fusion(fusion::Fusion),
  IndexNames(index_names::IndexNames),
}

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
