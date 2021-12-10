//! Code for processing and integrating book data.
//!
//! The book data tools are developed as a monolithic executable.  The commands
//! themselves live under [cli], while the rest of the package contains data
//! definitions and helper routines that build on this code.  The tools are not
//! currently usable as a library; you can extend them by adding additional commands
//! to the [cli] module.

pub mod cleaning;
pub mod parsing;
pub mod tsv;
pub mod util;
pub mod io;
pub mod ids;
pub mod gender;
pub mod graph;
pub mod marc;
pub mod rdf;
pub mod openlib;
pub mod amazon;
pub mod goodreads;
pub mod arrow;
pub mod interactions;
pub mod cli;
pub mod prelude;

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

use cli::CLI;

fn main() -> Result<()> {
  let opt = CLI::from_args();
  opt.exec()
}
