use anyhow::Result;
use structopt::StructOpt;
use log::*;

use super::Command;
use crate::db::DbOpts;

/// Dump environment info for debugging
#[derive(StructOpt, Debug)]
#[structopt(name="info")]
pub struct Info {
  #[structopt(flatten)]
  db: DbOpts
}

impl Command for Info {
  fn exec(self) -> Result<()> {
    let url = self.db.url()?;
    info!("DB_URL: {}", url);
    Ok(())
  }
}
