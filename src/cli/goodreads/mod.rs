use crate::prelude::*;

mod cluster;
mod scan;

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
  /// Cluster GoodReads intearaction data.
  ClusterInteractions(cluster::CICommand)
}

impl Command for Goodreads {
  fn exec(&self) -> Result<()> {
    match &self.command {
      GRCmd::Scan { data } => {
        data.exec()?;
      }
      GRCmd::ClusterInteractions(opts) => {
        opts.exec()?;
      },
    }

    Ok(())
  }
}
