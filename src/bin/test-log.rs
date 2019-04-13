use log::*;

use structopt::StructOpt;
use bookdata::LogOpts;

#[derive(StructOpt, Debug)]
#[structopt(name="test-log")]
struct Opt {
  #[structopt(flatten)]
  logging: LogOpts
}

fn main() {
  let opt = Opt::from_args();
  opt.logging.init().unwrap();

  eprintln!("testing log");
  error!("error message");
  warn!("warning message");
  info!("info message");
  debug!("debug message");
  trace!("debug message");
}
