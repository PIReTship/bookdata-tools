#[macro_use]
extern crate log;
#[macro_use] extern crate macro_attr;
#[macro_use] extern crate enum_derive;

extern crate quick_xml;
extern crate postgres;
extern crate ntriple;
extern crate zip;

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
