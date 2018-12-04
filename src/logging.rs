use structopt::StructOpt;
use indicatif::ProgressBar;
use log::*;

use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;

use error::Result;

static mut LOG_LEVEL: LevelFilter = LevelFilter::Info;
static LOG_PB: AtomicPtr<ProgressBar> = AtomicPtr::new(ptr::null_mut());
static LOGGER: LogEnv = LogEnv {};

struct LogEnv {}

impl Log for LogEnv {
  fn enabled(&self, metadata: &Metadata) -> bool {
    unsafe {
      metadata.level() <= LOG_LEVEL
    }
  }

  fn log(&self, record: &Record) {
    let pass = unsafe {
      record.level() <= LOG_LEVEL
    };
    if pass {
      let pb_ptr = LOG_PB.load(Ordering::Relaxed);
      if pb_ptr.is_null() {
        eprintln!("[{:>5}] {}", record.level(), record.args());
      } else {
        let msg = format!("[{:>5}] {}", record.level(), record.args());
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
        LOG_LEVEL = LevelFilter::Off;
      }
      for _i in 0..self.verbose {
        LOG_LEVEL = verbosify(LOG_LEVEL);
      }
    }
    set_logger(&LOGGER)?;
    unsafe {
      set_max_level(LOG_LEVEL);
    }
    Ok(())
  }
}

pub fn set_progress(pb: &ProgressBar) {
  let pbb = Box::new(pb.clone());
  LOG_PB.store(Box::leak(pbb), Ordering::Relaxed);
}

pub fn clear_progress() {
  LOG_PB.store(ptr::null_mut(), Ordering::Relaxed);
}

#[test]
fn test_default_logenv() {
  unsafe {
    assert!(Level::Info <= LOG_LEVEL);
    assert!(Level::Warn <= LOG_LEVEL);
    assert!(Level::Debug > LOG_LEVEL);
  }
}
