use std::fmt;
use std::io::Read;
use std::io::{Result as IOResult};
use std::fs::File;
use std::time::{Instant, Duration};
use friendly::temporal::HumanDuration;
use friendly::{scalar, bytes, duration};

use fallible_iterator::FallibleIterator;

use log::*;

/// Build and configure a [Timer] or a related structure.
///
/// This strucutre supports building timers and timed wrappers for things like iterators
/// and input streams.  It implements the [builder pattern][bp] for non-consuming builders.
///
/// [bp]: https://doc.rust-lang.org/1.12.0/style/ownership/builders.html
#[derive(Debug, Clone)]
pub struct TimerConfig {
  task_count: Option<usize>,
  interval: f32,
  log_prefix: Option<String>,
  log_target: String,
  started_at: Option<Instant>,
  count_format: fn(f64) -> Box<dyn fmt::Display>,
}

/// A timer for monitoring task completion.
#[derive(Debug, Clone)]
pub struct Timer {
  config: TimerConfig,
  started: Instant,
  completed: usize,
  last_write: LastWrite,
}

#[derive(Debug, Clone)]
enum LastWrite {
  Never,
  At {
    time: Instant,
    count: usize,
  }
}

/// Helper to format counts in decimal.
fn decimal_format(n: f64) -> Box<dyn fmt::Display> {
  Box::new(scalar(n))
}

/// Helper to format counts in binary.
fn bytes_format(n: f64) -> Box<dyn fmt::Display> {
  Box::new(bytes(n))
}


/// Struct wrapping an iterator to report progress.
///
/// This struct makes it easy to report progress to the logging infrastructure
/// while iterating. It can wrap either [Iterator] or [FallibleIterator], and
/// implements the same trait as the wrapped iterator type (type bounds on the
/// trait implementations accomplish this).
pub struct ProgressIter<I> {
  timer: Timer,
  iter: I
}

/// Report progress on reading data.
///
/// This struct supports [Timer]'s ability to wrap a [Read] and automatically
/// report progress as data passes through.
pub struct TimedRead<R: Read> {
  timer: Timer,
  reader: R,
  nlogs: usize,
}

impl TimerConfig {
  /// Start a new timer configuration.
  fn new() -> TimerConfig {
    TimerConfig {
      task_count: None,
      interval: 1.0,
      log_prefix: None,
      log_target: module_path!().to_string(),
      started_at: None,
      count_format: decimal_format,
    }
  }

  /// Convert this timer to use binary count formatting.
  pub fn binary(&mut self) -> &mut Self {
    self.count_format = bytes_format;
    self
  }

  /// Set the task count on this timer.
  pub fn task_count(&mut self, count: usize) -> &mut Self {
    self.task_count = Some(count);
    self
  }

  /// Set the default interval for this timer.
  pub fn interval(&mut self, interval: f32) -> &mut Self {
    self.interval = interval;
    self
  }

  /// Set the log label on this timer.
  pub fn label(&mut self, prefix: &str) -> &mut Self {
    self.log_prefix = Some(prefix.to_string());
    self
  }

  /// Set the log target on this timer.
  pub fn log_target(&mut self, target: &str) -> &mut Self {
    self.log_target = target.to_string();
    self
  }

  /// Configure an initial start time for this timer.
  pub fn start_time(&mut self, start: Instant) -> &mut Self {
    self.started_at = Some(start);
    self
  }

  /// Build a timer.
  pub fn build(&self) -> Timer {
    let config = self.clone();
    config.into_timer()
  }

  /// Internal helper to build without a clone, used for build methods that clone.
  fn into_timer(self) -> Timer {
    let started = self.started_at.clone().unwrap_or_else(Instant::now);
    Timer {
      config: self,
      started,
      completed: 0,
      last_write: LastWrite::Never,
    }
  }

  /// Create a wrapper that reports progress on reading a file. See [read_progress].
  pub fn file_progress(&self, file: File) -> IOResult<TimedRead<File>> {
    let meta = file.metadata()?;
    let mut clone = self.clone();
    clone.task_count(meta.len() as usize);
    clone.binary();
    Ok(TimedRead {
      reader: file,
      timer: clone.into_timer(),
      nlogs: 0,
    })
  }

  /// Use this timer to track progress in a reader.
  ///
  /// This method **copies** the timer into the progress reader.
  pub fn read_progress<R: Read>(&self, reader: R) -> TimedRead<R> {
    let mut clone = self.clone();
    clone.binary();
    TimedRead {
      reader,
      timer: clone.into_timer(),
      nlogs: 0,
    }
  }

  /// Emit progress from an iterator.
  pub fn iter_progress<I: Iterator>(&self, iter: I) -> ProgressIter<I> {
    let mut config = self.clone();
    // try for size
    let (lb, ub) = iter.size_hint();
    if let Some(n) = ub {
      config.task_count = Some(n);
    } else if lb > 0 {
      config.task_count = Some(lb);
    } else {
      config.task_count = None;
    }
    ProgressIter {
      timer: config.into_timer(),
      iter
    }
  }

  /// Emit progress from a fallible iterator.
  pub fn fallible_iter_progress<'a, I: FallibleIterator>(&self, iter: I) -> ProgressIter<I> {
    let mut config = self.clone();
    // try for size
    let (lb, ub) = iter.size_hint();
    if let Some(n) = ub {
      config.task_count = Some(n);
    } else if lb > 0 {
      config.task_count = Some(lb);
    } else {
      config.task_count = None;
    }
    ProgressIter {
      timer: config.into_timer(),
      iter
    }
  }
}

impl Timer {
  /// Create a new timer with defaults.
  pub fn new() -> Timer {
    Self::builder().build()
  }

  /// Create a new timer builder.
  pub fn builder() -> TimerConfig {
    TimerConfig::new()
  }

  /// Get a clone of the timer's configuration as a new builder.
  ///
  /// This is useful for doing things like wrapping iterators.
  pub fn copy_builder(&self) -> TimerConfig {
    let mut config = self.config.clone();
    config.start_time(self.started);
    config
  }

  /// Create a new timer with a task count.
  pub fn new_with_count(n: usize) -> Timer {
    Self::builder().task_count(n).build()
  }

  /// Advance the completed-task count.
  pub fn complete(&mut self, n: usize) {
    self.completed += n;
  }

  /// Check if we want to write progress updates.
  pub fn want_write(&self) -> bool {
    self.want_write_interval(self.config.interval)
  }

  /// Check if we want to write progress updates with a custom interval.
  pub fn want_write_interval(&self, interval_secs: f32) -> bool {
    let (lt, lc) = match self.last_write {
      LastWrite::Never => (self.started, 0),
      LastWrite::At { time, count } => (time, count)
    };
    // let's try to estimate intervals
    if lc > 0 {
      let ld = lt - self.started;
      let lsecs = ld.as_secs_f32();
      let lper = lsecs / (lc as f32);
      let diff = self.completed - lc;
      let est_elapsed = (diff as f32) * lper;
      if est_elapsed < interval_secs * 0.95 {
        // by quick estimation we don't need to check yet
        return false;
      }
    }

    // we couldn't estimate, let's get elapsed time.
    let since = lt.elapsed().as_secs_f32();
    // we want to write - it's been at least interval_secs since last write.
    since >= interval_secs
  }

  /// Record that a write happend.
  pub fn record_write(&mut self) {
    self.last_write = LastWrite::At {
      time: Instant::now(),
      count: self.completed,
    }
  }

  /// Write status to the logger at the timer's interval.
  pub fn maybe_log_status(&mut self) -> bool {
    self.maybe_log_status_interval(self.config.interval)
  }

  /// Write status to the logger at a specified interval.
  pub fn maybe_log_status_interval(&mut self, interval_secs: f32) -> bool {
    if self.want_write_interval(interval_secs) {
      self.log_status();
      self.record_write();
      true
    } else {
      false
    }
  }

  /// Log the current status unconditionally.
  ///
  /// This method does **not** call [record_write].
  pub fn log_status(&self) {
    if let Some(pfx) = &self.config.log_prefix {
      info!(target: &self.config.log_target, "{}: {}", pfx, self);
    } else {
      info!(target: &self.config.log_target, "{}", self);
    }
  }

  /// Log the current status unconditionally, with a message before the status.
  ///
  /// This method does **not** call [record_write].
  pub fn log_status_msg(&self, msg: &str) {
    if let Some(pfx) = &self.config.log_prefix {
      info!(target: &self.config.log_target, "{}: {} {}", pfx, msg, self);
    } else {
      info!(target: &self.config.log_target, "{} {}", msg, self);
    }
  }

  /// Get the elapsed time on this timer.
  pub fn elapsed(&self) -> Duration {
    self.started.elapsed()
  }

  /// Get the elapsed time on this timer, wrapped for human presentation.
  pub fn human_elapsed(&self) -> HumanDuration {
    self.elapsed().into()
  }

  /// Get the elapsed time and ETA on this timer.
  pub fn timings(&self) -> (Duration, Option<Duration>) {
    let elapsed = self.started.elapsed();
    match self.config.task_count {
      Some(n) if self.completed > 0 && self.completed < n => {
        let remaining = n - self.completed;
        let ds = elapsed.as_secs_f64();
        let per = ds / (self.completed as f64);
        let dr = per * (remaining as f64);
        (elapsed, Some(Duration::from_secs_f64(dr)))
      },
      _ => (elapsed, None)
    }
  }
}

impl fmt::Display for Timer {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let (el, eta) = self.timings();
    let per = (self.completed as f64) / el.as_secs_f64();
    if let Some(eta) = eta {
      write!(f, "{} / {} in {} ({}/s, ETA {})",
             (self.config.count_format)(self.completed as f64),
             (self.config.count_format)(self.config.task_count.unwrap_or_default() as f64),
             duration(el),
             (self.config.count_format)(per),
             duration(eta))
    } else if self.completed > 0 {
      write!(f, "{} in {} ({:.0}/s)", (self.config.count_format)(self.completed as f64), duration(el), (self.config.count_format)(per))
    } else {
      write!(f, "{}", self.human_elapsed())
    }
  }
}

impl <I> Iterator for ProgressIter<I> where I: Iterator {
  type Item = I::Item;

  fn next(&mut self) -> Option<I::Item> {
    let res = self.iter.next();
    if let Some(_) = res {
      self.timer.complete(1);
      self.timer.maybe_log_status();
    }
    res
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    self.iter.size_hint()
  }
}

impl <I> FallibleIterator for ProgressIter<I> where I: FallibleIterator {
  type Item = I::Item;
  type Error = I::Error;

  fn next(&mut self) -> Result<Option<I::Item>, I::Error> {
    let res = self.iter.next();
    if let Ok(Some(_)) = res {
      self.timer.complete(1);
      self.timer.maybe_log_status();
    }
    res
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    self.iter.size_hint()
  }
}

impl <R: Read> Read for TimedRead<R> {
  fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
    let size = self.reader.read(buf)?;
    self.timer.complete(size);

    let mut interval = self.timer.config.interval;
    if interval > 5.0 {
      if self.nlogs == 0 {
        interval = 5.0;
      } else if self.nlogs == 1 {
        interval -= 5.0;
      }
    }

    if size > 0 {
      if self.timer.maybe_log_status_interval(interval) {
        self.nlogs += 1;
      }
    } else {
      self.timer.log_status();
    }
    Ok(size)
  }
}

/// Format a duration with a human-readable string.
#[cfg(test)]
pub fn human_time(dur: Duration) -> String {
  let hd = HumanDuration::from(dur);
  hd.to_string()
}

#[test]
fn test_human_secs() {
  let s = human_time(Duration::from_secs(10));
  assert_eq!(s.as_str(), "10.00s");
}


#[test]
fn test_human_mins() {
  let s = human_time(Duration::from_secs(135));
  assert_eq!(s.as_str(), "2m15.00s");
}
