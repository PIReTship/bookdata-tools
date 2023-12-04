use friendly::temporal::HumanDuration;
use std::fmt;
use std::time::{Duration, Instant};

/// A timer for monitoring task completion.
#[derive(Debug, Clone)]
pub struct Timer {
    started: Instant,
}

impl Timer {
    /// Create a new timer with defaults.
    pub fn new() -> Timer {
        Timer {
            started: Instant::now(),
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
}

impl fmt::Display for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.human_elapsed())
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
