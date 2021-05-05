use std::time::Duration;

pub mod parsing;
mod accum;

pub use accum::DataAccumulator;

pub fn human_time(dur: Duration) -> String {
  let secs = dur.as_secs_f32();
  if secs > 3600.0 {
    let hrs = secs / 3600.0;
    let mins = (secs % 3600.0) / 60.0;
    let secs = secs % 60.0;
    format!("{}h{}m{:.2}s", hrs.floor(), mins.floor(), secs)
  } else if secs > 60.0 {
    let mins = secs / 60.0;
    let secs = secs % 60.0;
    format!("{}m{:.2}s", mins.floor(), secs)
  } else {
    format!("{:.2}s", secs)
  }
}
