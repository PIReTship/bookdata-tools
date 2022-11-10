use std::path::Path;
use std::fs::File;

use anyhow::Result;

use polars::prelude::*;

const ROW_GROUP_SIZE: usize = 1000_000;

/// Get a schema from a data frame with maximal nullability.
pub fn nonnull_schema(df: &DataFrame) -> ArrowSchema {
  let schema = df.schema().to_arrow();
  ArrowSchema {
    fields: schema.fields.into_iter().map(|f| {
      ArrowField {
        is_nullable: df.column(&f.name).expect("missing column").null_count() > 0,
        ..f
      }
    }).collect(),
    metadata: schema.metadata,
  }
}

pub fn save_df_parquet<P: AsRef<Path>>(df: DataFrame, path: P) -> Result<()> {
  let mut df = df;
  let file = File::create(path)?;
  ParquetWriter::new(file)
      .with_compression(ParquetCompression::Zstd(None))
      .with_row_group_size(Some(ROW_GROUP_SIZE))
      .finish(&mut df)?;
  Ok(())
}