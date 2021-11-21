pub use std::path::{Path, PathBuf};
pub use std::convert::{TryFrom, TryInto};
pub use anyhow::{Result, Error, anyhow};
pub use log::*;
pub use happylog::set_progress;
pub use structopt::StructOpt;
pub use fallible_iterator::FallibleIterator;
pub use crate::cli::CommonOpts;
pub use crate::io::LineProcessor;
pub use crate::io::progress::default_progress;
pub use crate::io::ObjectWriter;
pub use crate::io::file_size;
pub use crate::json_from_str;
pub use crate::util::Timer;
pub use crate::cli::Command;

pub use crate as bookdata;

/// Macro to implement FromStr using JSON.
#[macro_export]
macro_rules! json_from_str {
  ($name:ident) => {
    impl FromStr for $name {
      type Err = serde_json::Error;

      fn from_str(s: &str) -> serde_json::Result<$name> {
        return serde_json::from_str(s)
      }
    }
  }
}
