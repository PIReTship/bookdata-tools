mod cleaning;
mod tsv;
mod db;
mod io;
mod codes;
mod graph;
mod tracking;
mod index;
mod parquet;
mod commands;

use anyhow::{anyhow, Result};
use log::*;
use structopt::StructOpt;

use happylog::args::LogOpts;
use commands::*;

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
    app = app.subcommand(cmd.app().clone());
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
