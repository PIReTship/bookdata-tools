//! Code for processing and integrating book data.
//!
//! The `bookdata` crate (not published) provides the support and utility
//! code for the various programs used to integrate the book data.  If you
//! are writing additional integrations or analyses, you may find the
//! modules and functions in here useful.
use anyhow::{anyhow, Result};
use log::*;
use structopt::StructOpt;

use happylog::args::LogOpts;
use bookdata::cli::*;

/// BookData import tools
#[derive(StructOpt, Debug)]
#[structopt(name="bookdata")]
struct Opt {
  #[structopt(flatten)]
  logging: LogOpts
}

fn main() -> Result<()> {
  let mut app = Opt::clap();
  let cmds = commands();
  for cmd in &cmds {
    app = app.subcommand(cmd.clap());
  }
  let matches = app.get_matches();

  let opt = Opt::from_clap(&matches);
  opt.logging.init()?;
  let (sc_name, sc_app) = matches.subcommand();
  debug!("subcommand name {}", sc_name);
  for cmd in &cmds {
    if cmd.name() == sc_name {
      cmd.run(sc_app.ok_or(anyhow!("no options"))?)?
    }
  }
  Ok(())
}
