use std::fmt;
use std::time::{Instant, Duration};

use fallible_iterator::FallibleIterator;

use log::*;

/// A human-readable duration.
#[repr(transparent)]
#[derive(Debug)]
pub struct HumanDuration {
  dur: Duration
}

impl From<Duration> for HumanDuration {
  fn from(dur: Duration) -> HumanDuration {
    HumanDuration { dur }
  }
}

impl fmt::Display for HumanDuration {
  fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
    let secs = self.dur.as_secs_f32();
    if secs > 3600.0 {
      let hrs = secs / 3600.0;
      let mins = (secs % 3600.0) / 60.0;
      let secs = secs % 60.0;
      write!(fmt, "{}h{}m{:.2}s", hrs.floor(), mins.floor(), secs)
    } else if secs > 60.0 {
      let mins = secs / 60.0;
      let secs = secs % 60.0;
      write!(fmt, "{}m{:.2}s", mins.floor(), secs)
    } else {
      write!(fmt, "{:.2}s", secs)
    }
  }
}

/// A timer for monitoring task completion.
#[derive(Debug)]
pub struct Timer {
  started: Instant,
  task_count: Option<usize>,
  completed: usize,
  last_write: LastWrite,
}

#[derive(Debug)]
enum LastWrite {
  Never,
  At {
    time: Instant,
    count: usize,
  }
}

pub struct ProgressFailIter<'a, I> where I: FallibleIterator {
  timer: &'a mut Timer,
  prefix: &'a str,
  interval_secs: f32,
  iter: I
}

impl Timer {
  /// Create a new timer.
  pub fn new() -> Timer {
    Timer {
      started: Instant::now(),
      task_count: None,
      completed: 0,
      last_write: LastWrite::Never,
    }
  }

  /// Create a new timer with a task count.
  pub fn new_with_count(n: usize) -> Timer {
    let mut timer = Timer::new();
    timer.task_count = Some(n);
    timer
  }

  /// Advance the completed-task count.
  pub fn complete(&mut self, n: usize) {
    self.completed += n;
  }

  /// Check if we want to write progress updates.
  pub fn want_write(&self, interval_secs: f32) -> bool {
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

  /// Write status to the logger at specified interval.
  pub fn log_status(&mut self, prefix: &str, interval_secs: f32) {
    if self.want_write(interval_secs) {
      info!("{}: {}", prefix, self);
      self.record_write();
    }
  }

  /// Get the elapsed time on this timer.
  pub fn elapsed(&self) -> Duration {
    self.started.elapsed()
  }

  /// Get the elapsed time on this timer, wrapped for human presentation.
  pub fn human_elapsed(&self) -> HumanDuration {
    self.started.elapsed().into()
  }

  /// Get the elapsed time and ETA on this timer.
  pub fn timings(&self) -> (Duration, Option<Duration>) {
    let elapsed = self.started.elapsed();
    match self.task_count {
      Some(n) if self.completed > 0 => {
        let remaining = n - self.completed;
        let ds = elapsed.as_secs_f64();
        let per = ds / (self.completed as f64);
        let dr = per * (remaining as f64);
        (elapsed, Some(Duration::from_secs_f64(dr)))
      },
      _ => (elapsed, None)
    }
  }

  /// Emit progress from a fallible iterator
  pub fn fallible_iter_progress<'a, I: FallibleIterator>(&'a mut self, prefix: &'a str, interval_secs: f32, iter: I) -> ProgressFailIter<'a, I> {
    // try for size
    let (lb, ub) = iter.size_hint();
    if let Some(n) = ub {
      self.task_count = Some(n);
    } else if lb > 0 {
      self.task_count = Some(lb);
    } else {
      self.task_count = None;
    }
    ProgressFailIter {
      timer: self,
      prefix, interval_secs,
      iter
    }
  }
}

impl fmt::Display for Timer {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let (el, eta) = self.timings();
    let per = (self.completed as f64) / el.as_secs_f64();
    if let Some(eta) = eta {
      write!(f, "{} / {} in {} ({:.0}/s, ETA {})",
             self.completed, self.task_count.unwrap_or_default(),
             HumanDuration::from(el), per, HumanDuration::from(eta))
    } else if self.completed > 0 {
      write!(f, "{} in {} ({:.0}/s)", self.completed, HumanDuration::from(el), per)
    } else {
      write!(f, "{}", self.human_elapsed())
    }
  }
}

impl <'a, I> FallibleIterator for ProgressFailIter<'a, I> where I: FallibleIterator {
  type Item = I::Item;
  type Error = I::Error;

  fn next(&mut self) -> Result<Option<I::Item>, I::Error> {
    let res = self.iter.next();
    if let Ok(Some(_)) = res {
      self.timer.complete(1);
      self.timer.log_status(self.prefix, self.interval_secs);
    }
    res
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    self.iter.size_hint()
  }
}

/// Format a duration with a human-readable string.
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
