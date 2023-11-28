//! Logging utilities for the book data tools.
//!
//! This module contains support for initializing the logging infrastucture, and
//! for dynamically routing log messages based on whether there is an active
//! progress bar.

use std::fmt::Debug;
use std::marker::PhantomData;

use friendly::scalar;
use happylog::new_progress;
use indicatif::style::ProgressTracker;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};

const DATA_PROGRESS_TMPL: &str =
    "{prefix}: {wide_bar} {bytes}/{total_bytes} ({bytes_per_sec}, {elapsed} elapsed, ETA {eta})";
const ITEM_PROGRESS_TMPL: &str = "{prefix}: {wide_bar} {friendly_pos}/{friendly_len} ({friendly_rate}/s, {elapsed} elapsed, ETA {eta}) {msg}";
const METER_TMPL: &str = "{prefix}: {wide_bar} {pos}/{len} {msg}";

trait FieldExtract: Default + Send + Sync {
    fn extract(state: &ProgressState) -> f64;
}

#[derive(Default)]
struct Friendly<F: FieldExtract + 'static> {
    _ghost: PhantomData<F>,
}

impl<F: FieldExtract + 'static> ProgressTracker for Friendly<F> {
    fn clone_box(&self) -> Box<dyn ProgressTracker> {
        Box::new(Self::default())
    }

    fn reset(&mut self, _state: &indicatif::ProgressState, _now: std::time::Instant) {
        // do nothing
    }

    fn tick(&mut self, _state: &indicatif::ProgressState, _now: std::time::Instant) {
        // do nothing
    }

    fn write(&self, state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write) {
        let val = F::extract(state);
        let len = scalar(val);
        write!(w, "{}", len).expect("failed to write progress");
    }
}

#[derive(Default)]
struct Pos;
impl FieldExtract for Pos {
    fn extract(state: &ProgressState) -> f64 {
        state.pos() as f64
    }
}

#[derive(Default)]
struct Len;
impl FieldExtract for Len {
    fn extract(state: &ProgressState) -> f64 {
        state.len().map(|x| x as f64).unwrap_or(f64::NAN)
    }
}

#[derive(Default)]
struct Rate;
impl FieldExtract for Rate {
    fn extract(state: &ProgressState) -> f64 {
        state.per_sec()
    }
}

/// Create a progress bar for tracking data.
///
/// If the size is unknown at creation time, pass 0.
pub fn data_progress<S>(len: S) -> ProgressBar
where
    S: TryInto<u64>,
    S::Error: Debug,
{
    new_progress(len.try_into().expect("invalid length")).with_style(
        ProgressStyle::default_bar()
            .template(DATA_PROGRESS_TMPL)
            .expect("template error"),
    )
}

/// Create a progress bar for tracking items.
///
/// If the size is unknown at creation time, pass 0.
pub fn item_progress<S>(len: S, name: &str) -> ProgressBar
where
    S: TryInto<u64>,
    S::Error: Debug,
{
    let len: u64 = len.try_into().expect("invalid length");
    let len = Some(len).filter(|l| *l > 0);
    let style = ProgressStyle::default_bar()
        .with_key("friendly_pos", Friendly::<Pos>::default())
        .with_key("friendly_len", Friendly::<Len>::default())
        .with_key("friendly_rate", Friendly::<Rate>::default())
        .template(ITEM_PROGRESS_TMPL)
        .expect("template error");

    new_progress(len.unwrap_or(0))
        .with_style(style)
        .with_prefix(name.to_string())
}

/// Create a meter for monitoring pipelines.
pub fn meter<S>(len: S, name: &str) -> ProgressBar
where
    S: TryInto<u64>,
    S::Error: Debug,
{
    let len: u64 = len.try_into().expect("invalid length");
    let len = Some(len).filter(|l| *l > 0);
    let style = ProgressStyle::default_bar()
        .template(METER_TMPL)
        .expect("template error");
    new_progress(len.unwrap_or(0))
        .with_style(style)
        .with_prefix(name.to_string())
}
