use crate::prelude::*;

mod cluster;
mod scan;
mod link;

/// GoodReads processing commands.
#[derive(StructOpt, Debug)]
pub struct Goodreads {
  #[structopt(subcommand)]
  command: GRCmd
}

#[derive(StructOpt, Debug)]
enum GRCmd {
  /// Scan GoodReads data.
  Scan {
    #[structopt(subcommand)]
    data: scan::GRScan
  },
  /// Link GoodReads data.
  Link {
    #[structopt(subcommand)]
    data: link::GRLink
  },
  /// Cluster GoodReads intearaction data.
  ClusterInteractions(cluster::CICommand)
}

impl Command for Goodreads {
  fn exec(&self) -> Result<()> {
    match &self.command {
      GRCmd::Scan { data } => {
        data.exec()?;
      },
      GRCmd::Link { data } => {
        data.exec()?;
      },
      GRCmd::ClusterInteractions(opts) => {
        opts.exec()?;
      },
    }

    Ok(())
  }
}
