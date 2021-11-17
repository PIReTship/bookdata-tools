use std::sync::Arc;

use datafusion::prelude::*;
use datafusion::logical_plan::Expr;
use arrow::datatypes::*;

use bookdata::prelude::*;
use bookdata::ratings::*;
use bookdata::ids::codes::*;
use bookdata::arrow::fusion::coalesce;

use super::data::*;

static GR_INPUT_FILE: &'static str = "goodreads/gr-interactions.parquet";
static GR_LINK_FILE: &'static str = "goodreads/gr-book-link.parquet";

fn item_col(native: bool) -> Expr {
  if native {
    coalesce(vec![
      col("work_id") + lit(NS_GR_WORK.code()),
      col("book_id") + lit(NS_GR_BOOK.code())
    ]).alias("item")
  } else {
    col("cluster").alias("item")
  }
}

pub struct Ratings {
  native: bool
}

impl Ratings {
  pub fn new(native: bool) -> Ratings {
    Ratings {
      native
    }
  }
}

impl Source for Ratings {
  type Act = RatingRow;
  type DD = RatingDedup;

  fn scan_linked_actions(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    info!("setting up to scan GoodReads ratings from {}", GR_INPUT_FILE);

    let ratings = ctx.read_parquet(GR_INPUT_FILE)?;
    let ratings = ratings.filter(col("rating").is_not_null())?;
    let schema = ratings.schema();
    let num = DataType::Int64;
    let books = ctx.read_parquet(GR_LINK_FILE)?;
    let ratings = ratings.join(books, JoinType::Inner, &["book_id"], &["book_id"])?;
    let ratings = ratings.select(vec![
      col("user_id").alias("user"),
      item_col(self.native),
      (col("updated").cast_to(&num, schema)? / lit(1000)).alias("timestamp"),
      col("rating"),
    ])?;
    debug!("GR rating schema: {:#?}", ratings.schema());

    Ok(ratings)
  }

  fn has_timestamps(&self) -> bool {
    true
  }
}

pub struct Actions {
  native: bool
}

impl Actions {
  pub fn new(native: bool) -> Actions {
    Actions {
      native
    }
  }
}

impl Source for Actions {
  type Act = RatingRow;
  type DD = ActionDedup;

  fn scan_linked_actions(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    info!("setting up to scan GoodReads actions from {}", GR_INPUT_FILE);

    let ratings = ctx.read_parquet(GR_INPUT_FILE)?;
    let schema = ratings.schema();
    let num = DataType::Int64;
    let books = ctx.read_parquet(GR_LINK_FILE)?;
    let ratings = ratings.join(books, JoinType::Inner, &["book_id"], &["book_id"])?;
    let ratings = ratings.select(vec![
      col("user_id").alias("user"),
      item_col(self.native),
      (col("updated").cast_to(&num, schema)? / lit(1000)).alias("timestamp"),
      col("rating"),
    ])?;
    debug!("GR action schema: {}", ratings.schema());

    Ok(ratings)
  }

  fn has_timestamps(&self) -> bool {
    true
  }
}
