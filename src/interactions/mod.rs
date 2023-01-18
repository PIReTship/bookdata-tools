//! Manage and deduplicate ratings and interactions.
//!
//! This code consolidates rating de-duplication into a single place, so we can use the same
//! logic across data sets.  We always store timestamps, dropping them at output time, because
//! the largest data sets have timestamps.  Saving space for the smallest data set doesn't
//! seem worthwhile.
use anyhow::Result;
use std::path::Path;

mod actions;
mod ratings;

pub use actions::*;
pub use ratings::*;

/// Trait for an interaction.
pub trait Interaction {
    fn get_user(&self) -> i32;
    fn get_item(&self) -> i32;
    fn get_rating(&self) -> Option<f32>;
    fn get_timestamp(&self) -> i64;
}

/// Interface for de-duplicating interactions.
pub trait Dedup<I: Interaction> {
    /// Save an item in the deduplciator.
    fn add_interaction(&mut self, act: I) -> Result<()>;

    /// Write the de-duplicated reuslts to a file.
    fn save(&mut self, path: &Path) -> Result<usize>;
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct Key {
    user: i32,
    item: i32,
}

impl Key {
    fn new(user: i32, item: i32) -> Key {
        Key { user, item }
    }
}
