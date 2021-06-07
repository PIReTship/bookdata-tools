//! Manage and deduplicate ratings and interactions.
use std::path::Path;
use hashbrown::HashMap;

use log::*;
use anyhow::Result;

use bookdata::io::ObjectWriter;
use bookdata::arrow::*;
use crate as bookdata;

/// Record for a single output rating.
#[derive(TableRow, Debug)]
pub struct RatingRecord {
  user: u32,
  item: i32,
  rating: f32,
  last_rating: f32,
  timestamp: i64,
  last_time: i64,
  nratings: u32,
}

/// Record for a single output action.
#[derive(TableRow, Debug)]
pub struct ActionRecord {
  user: u32,
  item: i32,
  first_time: i64,
  last_time: i64,
  last_rating: Option<f32>,
  nactions: u32,
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct Key {
  user: u32,
  item: i32
}

impl Key {
  fn new(user: u32, item: i32) -> Key {
    Key {
      user, item
    }
  }
}

/// Rating deduplicator.
pub struct RatingDedup {
  table: HashMap<Key, Vec<(f32, i64)>>
}

impl RatingDedup {
  /// Create a new deduplicator.
  pub fn new() -> RatingDedup {
    RatingDedup {
      table: HashMap::new()
    }
  }

  /// Add a rating to the deduplicator.
  pub fn record(&mut self, user: u32, item: i32, rating: f32, timestamp: i64) {
    let k = Key::new(user, item);
    // get the vector for this user/item pair
    let vec = self.table.entry(k).or_insert_with(|| Vec::with_capacity(1));
    // and insert our records!
    vec.push((rating, timestamp));
  }

  /// Save the rating table disk.
  pub fn write_ratings<P: AsRef<Path>>(self, path: P) -> Result<usize> {
    info!("writing {} deduplicated ratings to {}", self.table.len(), path.as_ref().display());
    let mut writer = TableWriter::open(path)?;

    // we're going to consume the hashtable.
    for (k, vec) in self.table {
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
          nratings: vec.len() as u32
        })?;
      }
    }

    writer.finish()
  }
}


#[derive(PartialEq, Clone, Debug)]
struct Action {
  timestamp: i64,
  rating: Option<f32>,
}

/// Action deduplicator.
pub struct ActionDedup {
  table: HashMap<Key, Vec<Action>>
}

impl ActionDedup {
  /// Create a new deduplicator.
  pub fn new() -> ActionDedup {
    ActionDedup {
      table: HashMap::new()
    }
  }

  /// Add an action to the deduplicator.
  pub fn record(&mut self, user: u32, item: i32, timestamp: i64, rating: Option<f32>) {
    let k = Key::new(user, item);
    // get the vector for this user/item pair
    let vec = self.table.entry(k).or_insert_with(|| Vec::with_capacity(1));
    // and insert our records!
    vec.push(Action {
      timestamp, rating
    });
  }

  /// Save the rating table disk.
  pub fn write_actions<P: AsRef<Path>>(self, path: P) -> Result<usize> {
    info!("writing {} deduplicated actions to {}", self.table.len(), path.as_ref().display());
    let mut writer = TableWriter::open(path)?;

    // we're going to consume the hashtable.
    for (k, vec) in self.table {
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
          nactions: vec.len() as u32
        })?;
      }
    }

    writer.finish()
  }
}
