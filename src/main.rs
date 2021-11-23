//! Code for processing and integrating book data.
//!
//! The book data tools are developed as a monolithic executable.  The commands
//! themselves live under [cli], while the rest of the package contains data
//! definitions and helper routines that build on this code.  The tools are not
//! currently usable as a library; you can extend them by adding additional commands
//! to the [cli] module.

mod cleaning;
mod parsing;
mod tsv;
mod util;
mod io;
mod ids;
mod gender;
mod graph;
mod marc;
mod rdf;
mod openlib;
mod amazon;
mod goodreads;
mod arrow;
mod ratings;
mod cli;
mod prelude;

// jemalloc makes this code faster
#[cfg(target_env="gnu")]
use jemallocator::Jemalloc;

#[cfg(target_env="gnu")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// but jemalloc doesn't work on msvc, so use mimalloc
#[cfg(target_env="msvc")]
use mimalloc::MiMalloc;

#[cfg(target_env="msvc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use anyhow::Result;
use structopt::StructOpt;

use happylog::args::LogOpts;

use cli::{Command, BDCommand};

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
