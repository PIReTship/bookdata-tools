use structopt::StructOpt;
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
