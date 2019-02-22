#[macro_use]
extern crate log;
#[macro_use] extern crate derive_more;

extern crate structopt;
extern crate quick_xml;
extern crate postgres;
extern crate ntriple;
extern crate zip;
extern crate os_pipe;
extern crate crossbeam_channel;

mod error;
pub mod cleaning;
pub mod tsv;
pub mod db;

pub use error::BDError;
pub use error::Result;
pub use error::err;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct LogOpts {
  /// Verbose mode (-v, -vv, -vvv, etc.)
  #[structopt(short="v", long="verbose", parse(from_occurrences))]
  verbose: usize,
  /// Silence output
  #[structopt(short="q", long="quiet")]
  quiet: bool
}

impl LogOpts {
  /// Initialize logging
  pub fn init(&self) -> Result<()> {
    Ok(stderrlog::new().verbosity(self.verbose + 2).quiet(self.quiet).init()?)
  }
}
