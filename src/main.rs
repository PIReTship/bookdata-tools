//! Code for processing and integrating book data.
//!
//! The book data tools are developed as a monolithic executable.  The commands
//! themselves live under [cli], while the rest of the package contains data
//! definitions and helper routines that build on this code.  The tools are not
//! currently usable as a library; you can extend them by adding additional commands
//! to the [cli] module (`src/cli/` in the source tree).

mod cleaning;
mod parsing;
mod tsv;
mod util;
mod io;
mod ids;
mod gender;
mod layout;
mod graph;
mod marc;
mod openlib;
mod amazon;
mod goodreads;
mod arrow;
mod interactions;
mod cli;
mod prelude;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use anyhow::Result;
use clap::Parser;

use cli::CLI;

fn main() -> Result<()> {
  let opt = CLI::parse();
  opt.exec()
}
