//! Logging utilities for the book data tools.
//!
//! This module contains support for initializing the logging infrastucture, and
//! for dynamically routing log messages based on whether there is an active
//! progress bar.

use std::{fmt::Arguments, time::SystemTime, mem::MaybeUninit};

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

struct LogEnvironment {
  start: SystemTime,
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
  eprintln!("{}", record.args());
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

    let start = SystemTime::now();
    unsafe {
      // set up the logging environment
      LOG_ENV.write(LogEnvironment { start });
    }

    dispatch.apply()?;
    Ok(())
  }
}

