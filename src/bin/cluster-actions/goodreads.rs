use std::sync::Arc;

use datafusion::prelude::*;
use arrow::datatypes::*;

use bookdata::prelude::*;
use bookdata::ratings::*;

use super::data::*;

static GR_INPUT_FILE: &'static str = "goodreads/gr-interactions.parquet";

pub struct Ratings;

impl Source for Ratings {
  type Act = RatingRow;
  type DD = RatingDedup;

  fn scan_linked_actions(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    info!("setting up to scan GoodReads ratings from {}", GR_INPUT_FILE);

    let ratings = ctx.read_parquet(GR_INPUT_FILE)?;
    let ratings = ratings.filter(col("rating").is_not_null())?;
    let schema = ratings.schema();
    let num = DataType::Int64;
    let ratings = ratings.select(vec![
      col("user_id").alias("user"),
      col("cluster"),
      (col("updated").cast_to(&num, schema)? / lit(1000)).alias("timestamp"),
      col("rating"),
    ])?;

    Ok(ratings)
  }

  fn has_timestamps(&self) -> bool {
    true
  }
}

pub struct Actions;

impl Source for Actions {
  type Act = RatingRow;
  type DD = ActionDedup;

  fn scan_linked_actions(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    info!("setting up to scan GoodReads actions from {}", GR_INPUT_FILE);

    let ratings = ctx.read_parquet(GR_INPUT_FILE)?;
    let schema = ratings.schema();
    let num = DataType::Int64;
    let ratings = ratings.select(vec![
      col("user_id").alias("user"),
      col("cluster"),
      (col("updated").cast_to(&num, schema)? / lit(1000)).alias("timestamp"),
      col("rating"),
    ])?;

    Ok(ratings)
  }

  fn has_timestamps(&self) -> bool {
    true
  }
}
