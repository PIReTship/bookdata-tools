#[macro_use] extern crate log;
#[macro_use] extern crate derive_more;
#[macro_use] extern crate lazy_static;

extern crate structopt;
extern crate quick_xml;
extern crate postgres;
extern crate ntriple;
extern crate zip;
extern crate os_pipe;
extern crate crossbeam_channel;
extern crate console;
extern crate indicatif;

mod error;
pub mod cleaning;
pub mod tsv;
pub mod db;
pub mod logging;

pub use error::BDError;
pub use error::Result;
pub use error::err;
pub use logging::LogOpts;
