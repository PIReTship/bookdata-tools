use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem::take;
use std::path::Path;

use anyhow::{anyhow, Result};
use arrow2::array::TryExtend;
use log::*;

use super::{Dedup, Interaction, Key};
use crate::arrow::*;
use crate::io::{file_size, ObjectWriter};
use crate::util::logging::item_progress;
use crate::util::Timer;

/// Record for a single output rating.
#[derive(ArrowField, Debug)]
pub struct TimestampRatingRecord {
    pub user: i32,
    pub item: i32,
    pub rating: f32,
    pub last_rating: f32,
    pub timestamp: i64,
    pub last_time: i64,
    pub nratings: i32,
}

/// Record for a single output rating without time.
#[derive(ArrowField, Debug)]
pub struct TimelessRatingRecord {
    pub user: i32,
    pub item: i32,
    pub rating: f32,
    pub nratings: i32,
}

/// Collapse a sequence of ratings into a rating record.
pub trait FromRatingSet {
    fn create(user: i32, item: i32, ratings: Vec<(f32, i64)>) -> Self;
}

impl FromRatingSet for TimestampRatingRecord {
    fn create(user: i32, item: i32, ratings: Vec<(f32, i64)>) -> Self {
        let mut vec = ratings;
        if vec.len() == 1 {
            // fast path
            let (rating, timestamp) = vec[0];
            TimestampRatingRecord {
                user,
                item,
                rating,
                timestamp,
                last_rating: rating,
                last_time: timestamp,
                nratings: 1,
            }
        } else {
            vec.sort_unstable_by_key(|(r, _ts)| (r * 10.0) as i32);
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
            vec.sort_unstable_by_key(|(_r, ts)| *ts);
            let (last_rating, last_time) = vec[vec.len() - 1];

            TimestampRatingRecord {
                user,
                item,
                rating,
                timestamp,
                last_rating,
                last_time,
                nratings: vec.len() as i32,
            }
        }
    }
}

impl FromRatingSet for TimelessRatingRecord {
    fn create(user: i32, item: i32, ratings: Vec<(f32, i64)>) -> Self {
        let mut vec = ratings;
        if vec.len() == 1 {
            // fast path
            let (rating, _ts) = vec[0];
            TimelessRatingRecord {
                user,
                item,
                rating,
                nratings: 1,
            }
        } else {
            vec.sort_unstable_by_key(|(r, _ts)| (r * 10.0) as i32);
            let (rating, _ts) = if vec.len() % 2 == 0 {
                let mp_up = vec.len() / 2;
                // we need this and the previous
                let (r1, ts1) = vec[mp_up - 1];
                let (r2, ts2) = vec[mp_up];
                // and average
                ((r1 + r2) * 0.5, (ts1 + ts2) / 2)
            } else {
                vec[vec.len() / 2]
            };

            TimelessRatingRecord {
                user,
                item,
                rating,
                nratings: vec.len() as i32,
            }
        }
    }
}

/// Rating deduplicator.
pub struct RatingDedup<R>
where
    R: FromRatingSet,
{
    _phantom: PhantomData<R>,
    table: HashMap<Key, Vec<(f32, i64)>>,
}

impl<I: Interaction, R> Dedup<I> for RatingDedup<R>
where
    R: FromRatingSet + ArrowSerialize + Send + Sync + 'static,
    R::MutableArrayType: TryExtend<Option<R>>,
{
    fn add_interaction(&mut self, act: I) -> Result<()> {
        let rating = act
            .get_rating()
            .ok_or_else(|| anyhow!("rating deduplicator requires ratings"))?;
        self.record(act.get_user(), act.get_item(), rating, act.get_timestamp());
        Ok(())
    }

    fn save(&mut self, path: &Path) -> Result<usize> {
        self.write_ratings(path)
    }
}

impl<R> Default for RatingDedup<R>
where
    R: FromRatingSet + ArrowSerialize + Send + Sync + 'static,
    R::MutableArrayType: TryExtend<Option<R>>,
{
    fn default() -> RatingDedup<R> {
        RatingDedup {
            _phantom: PhantomData,
            table: HashMap::new(),
        }
    }
}

impl<R> RatingDedup<R>
where
    R: FromRatingSet + ArrowSerialize + Send + Sync + 'static,
    R::MutableArrayType: TryExtend<Option<R>>,
{
    /// Add a rating to the deduplicator.
    pub fn record(&mut self, user: i32, item: i32, rating: f32, timestamp: i64) {
        let k = Key::new(user, item);
        // get the vector for this user/item pair
        let vec = self.table.entry(k).or_insert_with(|| Vec::with_capacity(1));
        // and insert our records!
        vec.push((rating, timestamp));
    }

    /// Save the rating table disk.
    pub fn write_ratings<P: AsRef<Path>>(&mut self, path: P) -> Result<usize> {
        let path = path.as_ref();
        info!(
            "writing {} deduplicated ratings to {}",
            friendly::scalar(self.table.len()),
            path.display()
        );
        let mut writer = TableWriter::open(path)?;

        let n = self.table.len() as u64;
        let timer = Timer::new();
        let pb = item_progress(n, "writing ratings");

        // we're going to consume the hashtable.
        let table = take(&mut self.table);
        for (k, vec) in pb.wrap_iter(table.into_iter()) {
            let record = R::create(k.user, k.item, vec);
            writer.write_object(record)?;
        }

        let rv = writer.finish()?;
        pb.finish_and_clear();

        info!(
            "wrote {} ratings in {}, file is {}",
            friendly::scalar(n),
            timer.human_elapsed(),
            friendly::bytes(file_size(path)?)
        );

        Ok(rv)
    }
}
