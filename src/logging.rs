use structopt::StructOpt;
use indicatif::ProgressBar;
use log::*;

use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;

use error::Result;

static mut LOGGER: LogEnv = LogEnv {
  filter: LevelFilter::Info,
  progress: AtomicPtr::new(ptr::null_mut())
};

#[derive(Debug)]
struct LogEnv {
  filter: LevelFilter,
  progress: AtomicPtr<ProgressBar>
}

impl Log for LogEnv {
  fn enabled(&self, metadata: &Metadata) -> bool {
    metadata.level() <= self.filter
  }

  fn log(&self, record: &Record) {
    let pass = unsafe {
      record.level() <= LOGGER.filter
    };
    if pass {
      let msg = format!("{} - {}", record.level(), record.args());

      let pb_ptr = self.progress.load(Ordering::Relaxed);
      if pb_ptr.is_null() {
        eprintln!("{}", msg);
      } else {
        unsafe {
          let pb = &*pb_ptr;
          pb.println(msg)
        }
      }
    }
  }

  fn flush(&self) {}
}

fn verbosify(f: LevelFilter) -> LevelFilter {
  match f {
    LevelFilter::Error => LevelFilter::Warn,
    LevelFilter::Warn => LevelFilter::Info,
    LevelFilter::Info => LevelFilter::Debug,
    LevelFilter::Debug => LevelFilter::Trace,
    x => x
  }
}

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
    unsafe {
      if self.quiet {
        LOGGER.filter = LevelFilter::Off;
      }
      for _i in 0..self.verbose {
        LOGGER.filter = verbosify(LOGGER.filter);
      }
      log::set_logger(&LOGGER)?;
    }
    Ok(())
  }
}

pub fn set_progress(pb: &ProgressBar) {
  let pbb = Box::new(pb.clone());
  unsafe {
    LOGGER.progress.store(Box::leak(pbb), Ordering::Relaxed);
  }
}

pub fn clear_progress() {
  unsafe {
    LOGGER.progress.store(ptr::null_mut(), Ordering::Relaxed);
  }
}

#[test]
fn test_default_logenv() {
  let env = unsafe { &LOGGER };

  assert!(Level::Info <= env.filter);
  assert!(Level::Warn <= env.filter);
  assert!(Level::Debug > env.filter);
}
