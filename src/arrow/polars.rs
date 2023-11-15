use std::fs::File;
use std::path::Path;

use anyhow::Result;
use log::*;

use polars::prelude::*;

const ROW_GROUP_SIZE: usize = 1000_000;

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

/// Save a data frame to a Parquet file.
pub fn save_df_parquet<P: AsRef<Path>>(df: DataFrame, path: P) -> Result<()> {
    let path = path.as_ref();
    debug!("writing file {}", path.display());
    let mut df = df;
    let file = File::create(path)?;
    let size = ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_row_group_size(Some(ROW_GROUP_SIZE))
        .finish(&mut df)?;
    debug!("{}: wrote {}", path.display(), friendly::bytes(size));
    Ok(())
}

/// Scan a Parquet file into a data frame.
pub fn scan_df_parquet<P: AsRef<Path>>(file: P) -> Result<LazyFrame> {
    let file = file.as_ref();
    debug!("scanning file {}", file.display());
    let df = LazyFrame::scan_parquet(file, ScanArgsParquet::default())?;
    debug!("{}: schema {:?}", file.display(), df.schema()?);
    Ok(df)
}
