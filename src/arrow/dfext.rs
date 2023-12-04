//! Book data extensions to Polars.
use log::*;
use polars::prelude::*;

use crate::cleaning::names::clean_name;

pub fn udf_clean_name(col: Series) -> PolarsResult<Option<Series>> {
    let col = col.utf8()?;
    let res: Utf8Chunked = col
        .into_iter()
        .map(|n| {
            if let Some(s) = n {
                Some(clean_name(s))
            } else {
                None
            }
        })
        .collect();
    Ok(Some(res.into_series()))
}

/// Get a schema from a data frame with maximal nullability.
pub fn nonnull_schema(df: &DataFrame) -> ArrowSchema {
    let schema = df.schema().to_arrow();
    debug!("de-nullifying schema: {:?}", schema);
    let clean = ArrowSchema {
        fields: schema
            .fields
            .into_iter()
            .map(|f| ArrowField {
                is_nullable: df.column(&f.name).expect("missing column").null_count() > 0,
                ..f
            })
            .collect(),
        metadata: schema.metadata,
    };
    debug!("converted schema: {:?}", clean);
    clean
}
