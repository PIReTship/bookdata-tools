//! Logging utilities for the book data tools.
//!
//! This module contains support for initializing the logging infrastucture, and
//! for dynamically routing log messages based on whether there is an active
//! progress bar.

use std::fmt::Debug;

use indicatif::{ProgressBar, ProgressStyle};
use happylog::new_progress;

const DATA_PROGRESS_TMPL: &str = "{prefix}: {wide_bar} {bytes}/{total_bytes} ({bytes_per_sec}, {elapsed} elapsed, ETA {eta})";
const ITEM_PROGRESS_TMPL: &str = "{prefix}: {wide_bar} {human_pos}/{human_len} ({per_sec}, {elapsed} elapsed, ETA {eta}) {msg}";

/// Create a progress bar for tracking data.
///
/// If the size is unknown at creation time, pass 0.
pub fn data_progress<S>(len: S) -> ProgressBar
where S: TryInto<u64>,
  S::Error: Debug
{
  new_progress(len.try_into().expect("invalid length"))
    .with_style(ProgressStyle::default_bar().template(DATA_PROGRESS_TMPL).expect("template error"))
}

/// Create a progress bar for tracking items.
///
/// If the size is unknown at creation time, pass 0.
pub fn item_progress<S>(len: S, name: &str) -> ProgressBar
where S: TryInto<u64>,
  S::Error: Debug
{
  let len: u64 = len.try_into().expect("invalid length");
  let len = Some(len).filter(|l| *l > 0);
  new_progress(len.unwrap_or(0))
    .with_style(ProgressStyle::default_bar().template(ITEM_PROGRESS_TMPL).expect("template error"))
    .with_prefix(name.to_string())
}
