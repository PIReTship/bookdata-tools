//! Book data extensions to Polars.

use polars::prelude::*;

use crate::cleaning::names::clean_name;

pub fn udf_clean_name(col: Series) -> Result<Series> {
  let col = col.utf8()?;
  let res: Utf8Chunked = col.into_iter().map(|n| {
    if let Some(s) = n {
      Some(clean_name(s))
    } else {
      None
    }
  }).collect();
  Ok(res.into_series())
}
