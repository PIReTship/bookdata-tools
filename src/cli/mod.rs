//! Command line interface for book data.
//!
//! The book data tools are implemented as a single monolithic executable (to reduce
//! compilation time and disk space in common configurations, with different tools
//! implemented as subcommands.  Each subcommand implements the [Command] trait, which
//! exposes the command line arguments and invocation.
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

/// Enum to collect and dispatch CLI commands.
#[enum_dispatch(Command)]
#[derive(StructOpt, Debug)]
pub enum BDCommand {
  Fusion(fusion::Fusion),
  IndexNames(index_names::IndexNames),
  RDF(RDFWrapper)
}

/// Tools for processing RDF.
#[derive(StructOpt, Debug)]
pub struct RDFWrapper {
  #[structopt(subcommand)]
  command: RDFCommand
}

impl Command for RDFWrapper {
  fn exec(self) -> Result<()> {
    self.command.exec()
  }
}

#[enum_dispatch(Command)]
#[derive(StructOpt, Debug)]
pub enum RDFCommand {
  ScanNodes(rdf_scan_nodes::ScanNodes),
  ScanTriples(rdf_scan_triples::ScanTriples),
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
