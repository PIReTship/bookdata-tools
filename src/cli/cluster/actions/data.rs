use std::sync::Arc;

use serde::de::{DeserializeOwned};
use serde::Deserialize;

use datafusion::prelude::*;
use crate::prelude::*;
use crate::ratings::{Interaction, Dedup};

/// Trait for data sources.
pub trait Source {
  type Act: Interaction + DeserializeOwned;
  type DD: Dedup<Self::Act> + Default + 'static;

  fn scan_linked_actions(&self, ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>>;

  fn has_timestamps(&self) -> bool;

  fn make_dedup(&self) -> Self::DD {
    Self::DD::default()
  }
}

/// Generic rating row usable by most data sources.
#[derive(Deserialize)]
pub struct RatingRow {
  pub user: u32,
  pub item: i32,
  pub rating: Option<f32>,
  pub timestamp: i64
}

impl Interaction for RatingRow {
  fn get_user(&self) -> u32 {
    self.user
  }
  fn get_item(&self) -> i32 {
    self.item
  }
  fn get_rating(&self) -> Option<f32> {
    self.rating
  }
  fn get_timestamp(&self) -> i64 {
    self.timestamp
  }
}
