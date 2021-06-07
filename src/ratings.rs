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

/// Rating deduplicator.
pub struct RatingDedup {
  table: HashMap<(u32, i32), Vec<(f32, i64)>>
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
    let k = (user, item);
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
      let (user, item) = k;
      if vec.len() == 1 {
        // fast path
        let (rating, timestamp) = vec[0];
        writer.write_object(RatingRecord {
          user, item,
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
          user, item,
          rating, timestamp,
          last_rating, last_time,
          nratings: vec.len() as u32
        })?;
      }
    }

    writer.finish()
  }
}
