//! Extract basic information from a Parquet file.
use std::fmt::Debug;
use std::fs::File;
use std::io::{stdout, Read, Seek, Write};
use std::mem::drop;

use polars_arrow::datatypes::Field;
use serde::Serialize;

use friendly::{bytes, scalar};
use polars_parquet::read::schema::parquet_to_arrow_schema;
use polars_parquet::read::*;

use crate::prelude::*;

use super::Command;

/// Extract basic information from a Parquet file.
#[derive(Args, Debug)]
#[command(name = "collect-isbns")]
pub struct PQInfo {
    /// Check the length by decoding the file.
    #[arg(long = "check-length")]
    check_length: bool,

    /// Path to the output JSON file.
    #[arg(short = 'o', long = "output")]
    out_file: Option<PathBuf>,

    /// Path to the Parquet file.
    #[arg(name = "FILE")]
    source_file: PathBuf,
}

#[derive(Debug, Serialize)]
struct InfoStruct {
    row_count: usize,
    file_size: u64,
    fields: Vec<FieldStruct>,
}

#[derive(Debug, Serialize)]
struct FieldStruct {
    name: String,
    data_type: String,
    nullable: bool,
}

impl From<&Field> for FieldStruct {
    fn from(f: &Field) -> Self {
        FieldStruct {
            name: f.name.clone(),
            data_type: format!("{:?}", f.data_type),
            nullable: f.is_nullable,
        }
    }
}

fn check_length<R: Read + Seek>(reader: &mut FileReader<R>, expected: usize) -> Result<()> {
    let mut len = 0;
    for chunk in reader {
        let chunk = chunk?;
        let clen = chunk.len();
        let arrays = chunk.into_arrays();
        let types = arrays.iter().map(|a| a.data_type()).collect::<Vec<_>>();
        debug!("chunk length: {}", clen);
        debug!("chunk types: {:?}", types);
        len += clen;
    }

    if len != expected {
        warn!("expected {} rows but decoded {}", expected, len);
    }

    Ok(())
}

impl Command for PQInfo {
    fn exec(&self) -> Result<()> {
        info!("reading {:?}", self.source_file);

        let mut pqf = File::open(&self.source_file)?;
        let fmeta = pqf.metadata()?;
        info!("file size: {}", bytes(fmeta.len()));

        let meta = read_metadata(&mut pqf)?;

        info!("row count: {}", scalar(meta.num_rows));
        info!("row groups: {}", meta.row_groups.len());
        let rc2: usize = meta.row_groups.iter().map(|rg| rg.num_rows()).sum();
        if rc2 != meta.num_rows {
            warn!("row group total {} != file total {}", rc2, meta.num_rows);
        }

        info!("decoding schema");
        let schema = meta.schema();
        let fields = parquet_to_arrow_schema(schema.fields());

        let out = stdout();
        let mut ol = out.lock();
        for field in &fields {
            writeln!(&mut ol, "{:?}", field)?;
        }
        drop(ol);

        if let Some(ref file) = self.out_file {
            let mut out = File::create(file)?;
            let info = InfoStruct {
                row_count: meta.num_rows,
                file_size: fmeta.len() as u64,
                fields: fields.iter().map(|f| f.into()).collect(),
            };

            serde_json::to_writer_pretty(&mut out, &info)?;
            out.write(b"\n")?;
        }

        if self.check_length {
            let schema = infer_schema(&meta)?;
            let mut reader = FileReader::new(pqf, meta.row_groups, schema, None, None, None);
            check_length(&mut reader, meta.num_rows)?;
        }

        Ok(())
    }
}
