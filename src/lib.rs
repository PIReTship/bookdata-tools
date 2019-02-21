#[macro_use]
extern crate log;
#[macro_use] extern crate derive_more;

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

/// Initialize logging
pub fn log_init(quiet: bool, level: usize) -> Result<()> {
  Ok(stderrlog::new().verbosity(level + 2).quiet(quiet).init()?)
}
