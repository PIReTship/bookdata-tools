use std::sync::Arc;
use serde::Deserialize;
use datafusion::prelude::*;
use bookdata::prelude::*;
use bookdata::ratings::*;

use super::data::*;

#[derive(Deserialize, Debug, Clone)]
pub struct BXRating {
  user: u32,
  cluster: i32,
  rating: f32,
}

impl Interaction for BXRating {
  fn get_user(&self) -> u32 {
    self.user
  }
  fn get_item(&self) -> i32 {
    self.cluster
  }
  fn get_rating(&self) -> Option<f32> {
    if self.rating > 0.0 {
      Some(self.rating)
    } else {
      None
    }
  }
  fn get_timestamp(&self) -> i64 {
    -1
  }
}

pub struct Ratings;

impl Source for Ratings {
  type Act = BXRating;
  type DD = RatingDedup;

  fn scan_linked_actions(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    info!("setting up to scan BookCrossing ratings");
    // load linking info
    let ic_link = ctx.read_parquet("book-links/isbn-clusters.parquet")?;
    let ic_link = ic_link.select_columns(&["isbn", "cluster"])?;

    // load ratings
    let ratings = ctx.read_csv("bx/cleaned-ratings.csv", CsvReadOptions::new().has_header(true))?;
    let ratings = ratings.filter(col("rating").gt(lit(0)))?;
    let rlink = ratings.join(ic_link, JoinType::Inner, &["isbn"], &["isbn"])?;
    let rlink = rlink.select_columns(&["user", "cluster", "rating"])?;

    Ok(rlink)
  }

  fn has_timestamps(&self) -> bool {
    false
  }
}

pub struct Actions;

impl Source for Actions {
  type Act = BXRating;
  type DD = ActionDedup;

  fn scan_linked_actions(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    info!("setting up to scan BookCrossing actions");
    // load linking info
    let ic_link = ctx.read_parquet("book-links/isbn-clusters.parquet")?;
    let ic_link = ic_link.select_columns(&["isbn", "cluster"])?;

    // load ratings
    let ratings = ctx.read_csv("bx/cleaned-ratings.csv", CsvReadOptions::new().has_header(true))?;
    let rlink = ratings.join(ic_link, JoinType::Inner, &["isbn"], &["isbn"])?;
    let rlink = rlink.select_columns(&["user", "cluster", "rating"])?;

    Ok(rlink)
  }

  fn has_timestamps(&self) -> bool {
    false
  }
}
