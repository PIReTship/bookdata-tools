//! Logging utilities for the book data tools.
//!
//! This module contains support for initializing the logging infrastucture, and
//! for dynamically routing log messages based on whether there is an active
//! progress bar.

use std::{fmt::Arguments, time::SystemTime, mem::MaybeUninit, sync::RwLock};

use indicatif::ProgressBar;
use log::*;
use fern::*;
use colored::*;
use chrono::{DateTime, Local};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct LogOptions {
  /// Increase output verbosity
  #[structopt(short="v", long="verbose", parse(from_occurrences))]
  pub verbose: i32,

  /// Suppress informational output
  #[structopt(short="q", long="quiet")]
  pub quiet: bool,
}

/// Guard struct for redirecting log output e.g. to a progress bar.
///
/// Resets the log output when dropped.  See [set_progress].
pub struct LogStateGuard {
  prev: Target,
}

#[derive(Debug, Clone)]
enum Target {
  Stderr,
  PB(ProgressBar),
}

struct LogEnvironment {
  target: RwLock<Target>
}

static mut LOG_ENV: MaybeUninit<LogEnvironment> = MaybeUninit::uninit();

fn color_level(level: Level) -> ColoredString {
  let str = format!("{}", level);
  match level {
    Level::Error => str.red().bold(),
    Level::Warn => str.yellow().bold(),
    Level::Info => str.blue().bold(),
    Level::Debug => str.white(),
    Level::Trace => str.white(),
  }
}

fn write_console_log(record: &Record<'_>) {
  let env = unsafe {
    LOG_ENV.assume_init_ref()
  };
  let lock = env.target.read().expect("poisoned lock");
  let target = &*lock;
  match target {
    Target::Stderr => eprintln!("{}", record.args()),
    Target::PB(pb) => pb.println(format!("{}", record.args())),
  }
}

fn format_console_log(out: FormatCallback<'_>, message: &Arguments<'_>, record: &Record<'_>) {
  let time: DateTime<Local> = SystemTime::now().into();
  let time = time.time();
  out.finish(format_args!(
    "[{}] ({}) {}",
    color_level(record.level()),
    time.format("%T").to_string().bold(),
    message
  ));
}

impl LogOptions {
  /// Initialize the logging infrastructure.
  pub fn setup(&self) -> Result<(), fern::InitError> {
    let mut dispatch = Dispatch::new();

    if self.quiet {
      dispatch = dispatch.level(LevelFilter::Error);
    } else if self.verbose == 1 {
      dispatch = dispatch.level(LevelFilter::Debug).level_for("datafusion", LevelFilter::Info);
    } else if self.verbose == 2 {
      dispatch = dispatch.level(LevelFilter::Debug);
    } else if self.verbose >= 3 {
      dispatch = dispatch.level(LevelFilter::Trace);
    } else {
      dispatch = dispatch.level(LevelFilter::Info);
    }

    let dispatch = dispatch.format(format_console_log);
    let dispatch = dispatch.chain(Output::call(write_console_log));

    let target = RwLock::new(Target::Stderr);
    unsafe {
      // set up the logging environment
      LOG_ENV.write(LogEnvironment { target });
    }

    dispatch.apply()?;
    Ok(())
  }
}

/// Temporarily redirect output to a progress bar.
///
/// If you are using a progress bar, this will set the logger to write through it to
/// coordinate log output and progress output.
///
/// ```
/// let pb = ProgressBar::new();
/// let _lg = set_progress(pb)
/// // do things
/// // log reset when _lg is dropped
/// ```
pub fn set_progress(pb: ProgressBar) -> LogStateGuard {
  let env = unsafe {
    LOG_ENV.assume_init_ref()
  };
  let mut target = env.target.write().expect("lock poisoned");
  let prev = target.clone();
  *target = Target::PB(pb);
  LogStateGuard { prev }
}

impl Drop for LogStateGuard {
  fn drop(&mut self) {
    let env = unsafe {
      LOG_ENV.assume_init_ref()
    };
    let mut target = env.target.write().expect("lock poisoned");
    *target = self.prev.clone();
  }
}
