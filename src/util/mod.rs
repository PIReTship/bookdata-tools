//! Various utility modules.
use std::path::Path;
use std::env::current_dir;

use log::*;
use anyhow::*;

mod accum;
#[cfg(unix)]
pub mod process;
pub mod timing;
pub mod serde_string;
pub mod logging;
pub mod iteration;
pub mod unicode;

pub use accum::StringAccumulator;
pub use timing::Timer;

/// Free default function for easily constructiong defaults.
pub fn default<T: Default>() -> T {
  T::default()
}

/// Expect that we are in a particular subdirectory.
pub fn require_working_dir<P: AsRef<Path>>(path: P) -> anyhow::Result<()> {
  let path = path.as_ref();
  let cwd = current_dir()?;

  if !cwd.ends_with(path) {
    error!("expected to be run in {:?}", path);
    Err(anyhow!("incorrect working directory"))
  } else {
    Ok(())
  }
}
