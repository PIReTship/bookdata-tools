use std::sync::Arc;
use datafusion::prelude::*;
use bookdata::prelude::*;

use super::Source;

pub struct Ratings;

impl Source for Ratings {
  fn scan_linked_actions(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    info!("setting up to scan Amazon ratings");
    // load linking info
    let ic_link = ctx.read_parquet("book-links/isbn-clusters.parquet")?;
    let ic_link = ic_link.select_columns(&["isbn", "cluster"])?;

    // load ratings
    let ratings = ctx.read_parquet("az2014/ratings.parquet")?;
    let rlink = ratings.join(ic_link, JoinType::Inner, &["asin"], &["isbn"])?;
    let rlink = rlink.select_columns(&["user", "cluster", "rating", "timestamp"])?;

    Ok(rlink)
  }

  fn has_timestamps(&self) -> bool {
    true
  }

  fn is_ratings(&self) -> bool {
    true
  }
}
