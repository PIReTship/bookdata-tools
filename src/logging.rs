use structopt::StructOpt;
use indicatif::ProgressBar;
use log::*;

use std::sync::Mutex;

use error::Result;

static mut LOGGER: LogEnv = LogEnv {
  filter: LevelFilter::Info
};

lazy_static! {
  static ref LOG_PB: Mutex<Option<ProgressBar>> = Mutex::new(None);
}

#[derive(Debug, Clone)]
struct LogEnv {
  filter: LevelFilter
}

impl Log for LogEnv {
  fn enabled(&self, metadata: &Metadata) -> bool {
    metadata.level() <= self.filter
  }

  fn log(&self, record: &Record) {
    let pass = unsafe {
      record.level() <= LOGGER.filter
    };
    if !pass { return; }
    let msg = format!("{} - {}", record.level(), record.args());
    let lock = LOG_PB.lock().unwrap();
    let progress = &*lock;
    match progress {
      Some(ref pb) => pb.println(msg),
      None => eprintln!("{}", msg)
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
  let mut lock = LOG_PB.lock().unwrap();
  let opb = &mut *lock;
  let _old = opb.replace(pb.clone());
}

pub fn clear_progress() {
  let mut lock = LOG_PB.lock().unwrap();
  let opb = &mut *lock;
  let _old = opb.take();
}

#[test]
fn test_default_logenv() {
  let env = unsafe { &LOGGER };

  assert!(Level::Info <= env.filter);
  assert!(Level::Warn <= env.filter);
  assert!(Level::Debug > env.filter);
}
