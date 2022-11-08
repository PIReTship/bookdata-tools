pub mod reader;
pub mod writer;
pub mod dfext;
pub mod polars;

pub use reader::scan_parquet_file;
pub use arrow2_convert::ArrowField;
pub use arrow2_convert::serialize::ArrowSerialize;
pub use writer::TableWriter;

use arrow2::datatypes::{Schema, Field};

/// Convert an Arrow schema so all rows are non-nullable.
pub fn nonnull_schema(schema: Schema) -> Schema {
  Schema {
    fields: schema.fields.into_iter().map(|f| {
      Field {
        is_nullable: false,
        ..f
      }
    }).collect(),
    metadata: schema.metadata,
  }
}
