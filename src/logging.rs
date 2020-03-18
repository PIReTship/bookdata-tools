use structopt::StructOpt;
use indicatif::ProgressBar;
use log::*;

use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;
use std::marker::PhantomData;

use anyhow::Result;

static LOG_PB: AtomicPtr<ProgressBar> = AtomicPtr::new(ptr::null_mut());

/// Progress bar logging context
pub struct LogPBState<'a> {
  phantom: PhantomData<&'a str>
}

struct LogEnv {
  level: LevelFilter,
  progress: &'static AtomicPtr<ProgressBar>
}

impl Log for LogEnv {
  fn enabled(&self, metadata: &Metadata) -> bool {
    metadata.level() <= self.level
  }

  fn log(&self, record: &Record) {
    let pass = record.level() <= self.level;
    if pass {
      let pb_ptr = self.progress.load(Ordering::Relaxed);
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
    let mut level = LevelFilter::Info;
    if self.quiet {
      level = LevelFilter::Off;
    }
    for _i in 0..self.verbose {
      level = verbosify(level);
    }
    let logger = LogEnv {
      level: level,
      progress: &LOG_PB
    };
    set_boxed_logger(Box::new(logger))?;
    set_max_level(level);
    Ok(())
  }
}

pub fn set_progress<'a>(pb: &'a ProgressBar) -> LogPBState<'a> {
  let pbb = Box::new(pb.clone());
  LOG_PB.store(Box::leak(pbb), Ordering::Relaxed);
  LogPBState {
    phantom: PhantomData
  }
}

impl <'a> Drop for LogPBState<'a> {
  fn drop(&mut self) {
    LOG_PB.store(ptr::null_mut(), Ordering::Relaxed);
  }
}
