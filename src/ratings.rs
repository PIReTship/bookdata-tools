//! Manage and deduplicate ratings and interactions.
//!
//! This code consolidates rating de-duplication into a single place, so we can use the same
//! logic across data sets.  We always store timestamps, dropping them at output time, because
//! the largest data sets have timestamps.  Saving space for the smallest data set doesn't
//! seem worthwhile.
use std::path::Path;
use hashbrown::HashMap;
use std::mem::take;

use log::*;
use anyhow::{Result, anyhow};
use friendly;

use bookdata::io::{ObjectWriter, file_size};
use bookdata::arrow::*;
use bookdata::util::Timer;
use crate as bookdata;

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
  fn save(&mut self, path: &Path, times: bool) -> Result<usize>;
}

/// Record for a single output rating.
#[derive(ParquetRecordWriter, Debug)]
pub struct RatingRecord {
  user: i32,
  item: i32,
  rating: f32,
  last_rating: f32,
  timestamp: i64,
  last_time: i64,
  nratings: i32,
}

/// Record for a single output rating without time.
#[derive(ParquetRecordWriter, Debug)]
pub struct TimelessRatingRecord {
  user: i32,
  item: i32,
  rating: f32,
  nratings: i32,
}

/// Record for a single output action.
#[derive(ParquetRecordWriter, Debug)]
pub struct ActionRecord {
  user: i32,
  item: i32,
  first_time: i64,
  last_time: i64,
  last_rating: Option<f32>,
  nactions: i32,
}

/// Record for a single output action without time.
#[derive(ParquetRecordWriter, Debug)]
pub struct TimelessActionRecord {
  user: i32,
  item: i32,
  nactions: i32,
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct Key {
  user: i32,
  item: i32
}

impl Key {
  fn new(user: i32, item: i32) -> Key {
    Key {
      user, item
    }
  }
}

/// Rating deduplicator.
#[derive(Default)]
pub struct RatingDedup {
  table: HashMap<Key, Vec<(f32, i64)>>
}

impl <I: Interaction> Dedup<I> for RatingDedup {
  fn add_interaction(&mut self, act: I) -> Result<()> {
    let rating = act.get_rating().ok_or_else(|| {
      anyhow!("rating deduplicator requires ratings")
    })?;
    self.record(act.get_user(), act.get_item(), rating, act.get_timestamp());
    Ok(())
  }

  fn save(&mut self, path: &Path, times: bool) -> Result<usize> {
    self.write_ratings(path, times)
  }
}

impl RatingDedup {
  /// Add a rating to the deduplicator.
  pub fn record(&mut self, user: i32, item: i32, rating: f32, timestamp: i64) {
    let k = Key::new(user, item);
    // get the vector for this user/item pair
    let vec = self.table.entry(k).or_insert_with(|| Vec::with_capacity(1));
    // and insert our records!
    vec.push((rating, timestamp));
  }

  /// Save the rating table disk.
  pub fn write_ratings<P: AsRef<Path>>(&mut self, path: P, times: bool) -> Result<usize> {
    let path = path.as_ref();
    info!("writing {} deduplicated ratings to {}",
          friendly::scalar(self.table.len()),
          path.display());
    let mut twb = TableWriterBuilder::new()?;
    if !times {
      // twb = twb.project(&["user", "item", "rating", "nratings"]);
    }
    let mut writer = twb.open(path)?;

    let mut timer = Timer::new_with_count(self.table.len());

    // we're going to consume the hashtable.
    for (k, vec) in take(&mut self.table) {
      let mut vec = vec;
      if vec.len() == 1 {
        // fast path
        let (rating, timestamp) = vec[0];
        writer.write_object(RatingRecord {
          user: k.user,
          item: k.item,
          rating, timestamp,
          last_rating: rating,
          last_time: timestamp,
          nratings: 1
        })?;
      } else {
        vec.sort_unstable_by_key(|(_r,ts)| *ts);
        let (rating, timestamp) = if vec.len() % 2 == 0 {
          let mp_up = vec.len() / 2;
          // we need this and the previous
          let (r1, ts1) = vec[mp_up - 1];
          let (r2, ts2) = vec[mp_up];
          // and average
          ((r1 + r2) * 0.5, (ts1 + ts2) / 2)
        } else {
          vec[vec.len() / 2]
        };
        let (last_rating, last_time) = vec[vec.len() - 1];

        writer.write_object(RatingRecord {
          user: k.user,
          item: k.item,
          rating, timestamp,
          last_rating, last_time,
          nratings: vec.len() as i32
        })?;
      }
      timer.complete(1);
      timer.log_status("writing ratings", 5.0);
    }

    let rv = writer.finish()?;

    info!("wrote ratings in {}, file is {}",
          timer.human_elapsed(),
          friendly::bytes(file_size(path)?));

    Ok(rv)
  }
}


#[derive(PartialEq, Clone, Debug)]
struct ActionInstance {
  timestamp: i64,
  rating: Option<f32>,
}

/// Action deduplicator.
#[derive(Default)]
pub struct ActionDedup {
  table: HashMap<Key, Vec<ActionInstance>>
}

impl <I: Interaction> Dedup<I> for ActionDedup {
  fn add_interaction(&mut self, act: I) -> Result<()> {
    self.record(act.get_user(), act.get_item(), act.get_timestamp(), act.get_rating());
    Ok(())
  }

  fn save(&mut self, path: &Path, times: bool) -> Result<usize> {
    self.write_actions(path, times)
  }
}

impl ActionDedup {
  /// Add an action to the deduplicator.
  pub fn record(&mut self, user: i32, item: i32, timestamp: i64, rating: Option<f32>) {
    let k = Key::new(user, item);
    // get the vector for this user/item pair
    let vec = self.table.entry(k).or_insert_with(|| Vec::with_capacity(1));
    // and insert our records!
    vec.push(ActionInstance {
      timestamp, rating
    });
  }

  /// Save the rating table disk.
  pub fn write_actions<P: AsRef<Path>>(&mut self, path: P, times: bool) -> Result<usize> {
    let path = path.as_ref();
    info!("writing {} deduplicated actions to {}",
          friendly::scalar(self.table.len()),
          path.display());
    let mut twb = TableWriterBuilder::new()?;
    if !times {
      // twb = twb.project(&["user", "item", "nactions"]);
    }
    let mut writer = twb.open(path)?;
    let mut timer = Timer::new_with_count(self.table.len());

    // we're going to consume the hashtable.
    for (k, vec) in take(&mut self.table) {
      let mut vec = vec;
      if vec.len() == 1 {
        // fast path
        let act = &vec[0];
        writer.write_object(ActionRecord {
          user: k.user,
          item: k.item,
          first_time: act.timestamp,
          last_time: act.timestamp,
          last_rating: act.rating,
          nactions: 1
        })?;
      } else {
        vec.sort_unstable_by_key(|a| a.timestamp);
        let first = &vec[0];
        let last = &vec[vec.len() - 1];
        let rates = vec.iter().flat_map(|a| a.rating).collect::<Vec<f32>>();
        let last_rating = if rates.len() > 0 {
          Some(rates[rates.len() - 1])
        } else {
          None
        };

        writer.write_object(ActionRecord {
          user: k.user,
          item: k.item,
          first_time: first.timestamp,
          last_time: last.timestamp,
          last_rating,
          nactions: vec.len() as i32
        })?;
      }

      timer.complete(1);
      timer.log_status("writing actions", 5.0);
    }

    let rv = writer.finish()?;

    info!("wrote ratings in {}, file is {}",
          timer.human_elapsed(),
          friendly::bytes(file_size(path)?));

    Ok(rv)
  }
}
