use std::sync::Arc;
use datafusion::prelude::*;
use crate::prelude::*;
use crate::ratings::RatingDedup;

use super::data::*;

pub struct Ratings;

#[async_trait]
impl Source for Ratings {
  type Act = RatingRow;
  type DD = RatingDedup;

  async fn scan_linked_actions(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    info!("setting up to scan Amazon ratings");

    // load linking info
    let ic_link = ctx.read_parquet("book-links/isbn-clusters.parquet");
    let ic_link = ic_link.await?;
    let ic_link = ic_link.select_columns(&["isbn", "cluster"])?;

    // load ratings
    let ratings = ctx.read_parquet("az2014/ratings.parquet");
    let ratings = ratings.await?;
    let rlink = ratings.join(ic_link, JoinType::Inner, &["asin"], &["isbn"])?;
    let rlink = rlink.select(vec![
      col("user"),
      col("cluster").alias("item"),
      col("rating"),
      col("timestamp")
    ])?;

    Ok(rlink)
  }

  fn has_timestamps(&self) -> bool {
    true
  }
}
