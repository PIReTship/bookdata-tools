use std::sync::Arc;
use datafusion::prelude::*;
use crate::prelude::*;
use crate::ratings::RatingDedup;

use super::data::*;

static ISBN_CLUSTER_FILE: &'static str = "book-links/isbn-clusters.parquet";
static AZ_INPUT_FILE: &'static str = "az2014/ratings.parquet";

pub struct Ratings {
  path: String
}

impl Ratings {
  pub fn new(src: Option<&str>) -> Ratings {
    let path = match src {
      Some(p) => p.to_string(),
      None => AZ_INPUT_FILE.into()
    };
    Ratings {
      path
    }
  }
}

#[async_trait]
impl Source for Ratings {
  type Act = RatingRow;
  type DD = RatingDedup;

  async fn scan_linked_actions(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
    info!("setting up to scan Amazon ratings from {}", self.path);

    // load linking info
    let ic_link = ctx.read_parquet(ISBN_CLUSTER_FILE).await?;
    let ic_link = ic_link.select_columns(&["isbn", "cluster"])?;

    // load ratings
    let ratings = ctx.read_parquet(&self.path);
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
