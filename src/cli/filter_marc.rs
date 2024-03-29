//! Command to filter MARC output.
use std::fs::File;
use std::io::Write;
use std::mem;
use std::path::{Path, PathBuf};
use std::thread::{spawn, JoinHandle};

use crossbeam::channel::{bounded, Receiver, Sender};
use friendly::scalar;
use polars::io::parquet::BatchedWriter;
use polars::prelude::*;

use crate::arrow::scan_parquet_file;
use crate::marc::flat_fields::FieldRecord;
use crate::prelude::*;

const BATCH_SIZE: usize = 1024 * 1024;

/// Filter a MARC field file to only contain certain results.
#[derive(Args, Debug)]
#[command(name = "filter-marc")]
pub struct FilterMARC {
    #[command(flatten)]
    filter: FilterSpec,

    #[command(flatten)]
    output: OutputSpec,

    /// Input file of MARC field data.
    #[arg(name = "FIELD_FILE")]
    field_file: PathBuf,
}

/// Options for filtering MARC records.
#[derive(Args, Debug, Clone)]
struct FilterSpec {
    /// Specify the tag to filter to.
    #[arg(short = 't', long = "tag", name = "TAG")]
    tag: Option<i16>,

    /// Specify the subfield to filter to.
    #[arg(short = 'f', long = "subfield", name = "CODE")]
    subfield: Option<char>,

    /// Trim the contents before emitting.
    #[arg(short = 'T', long = "trim")]
    trim: bool,

    /// Lowercase the contents before emitting.
    #[arg(short = 'L', long = "lower")]
    lower: bool,
}

/// Options for output.
#[derive(Args, Debug, Clone)]
struct OutputSpec {
    /// Rename the content field.
    #[arg(short = 'n', long = "name", name = "FIELD")]
    content_name: Option<String>,

    /// Output file for filtered MARC fields.
    #[arg(short = 'o', long = "output", name = "FILE")]
    file: PathBuf,
}

impl FilterSpec {
    fn matches(&self, rec: &FieldRecord) -> bool {
        if let Some(t) = &self.tag {
            if rec.tag != *t {
                return false;
            }
        }

        if let Some(sf) = &self.subfield {
            if rec.sf_code != (*sf as u8) {
                return false;
            }
        }

        true
    }

    fn transform<'a>(&self, value: &'a str) -> Cow<'a, str> {
        let content: Cow<'a, str> = if self.trim {
            value.trim().into()
        } else {
            value.into()
        };

        let content: Cow<'a, str> = if self.lower {
            content.to_lowercase().into()
        } else {
            content
        };

        content
    }
}

/// Scan MARC records from a file.
///
/// Failes quickly if there is an error opening the file; errors reading the file are
/// from the thread and are availabl when it is joined.
fn scan_records(
    path: &Path,
    filter: &FilterSpec,
    send: Sender<FieldRecord>,
) -> Result<JoinHandle<Result<usize>>> {
    info!("reading names from authority fields in {:?}", path);
    let scanner = scan_parquet_file(path)?;
    let filter = filter.clone(); // to transfer to thread

    Ok(spawn(move || {
        let scanner = scanner;
        let mut n = 0;
        for rec in scanner {
            n += 1;
            let mut rec: FieldRecord = rec?;
            if filter.matches(&rec) {
                rec.contents = filter.transform(rec.contents.as_str()).into();
                send.send(rec)?;
            }
        }
        debug!("finished scanning parquet");
        Ok(n)
    }))
}

/// Write field records to an output file.
fn write_records(out: &OutputSpec, recv: Receiver<FieldRecord>) -> Result<usize> {
    info!("writing output to {:?}", out.file);
    let out_name = out
        .content_name
        .as_ref()
        .map(|s| s.clone())
        .unwrap_or("content".into());
    let mut schema = Schema::new();
    schema.with_column("rec_id".into(), DataType::UInt32);
    schema.with_column(out_name.as_str().into(), DataType::String);

    let writer = File::options()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&out.file)?;
    let mut writer = ParquetWriter::new(writer)
        .with_compression(ParquetCompression::Zstd(None))
        .with_statistics(true)
        .batched(&schema)?;

    let mut rec_ids = Vec::with_capacity(BATCH_SIZE);
    let mut values = Vec::with_capacity(BATCH_SIZE);
    let mut n = 0;

    for rec in recv {
        rec_ids.push(rec.rec_id);
        values.push(rec.contents);
        if rec_ids.len() >= BATCH_SIZE {
            n += write_cols(&mut writer, &mut rec_ids, &mut values, &out_name)?;
            rec_ids.clear();
            values.clear();
        }
    }

    n += write_cols(&mut writer, &mut rec_ids, &mut values, &out_name)?;

    writer.finish()?;

    Ok(n)
}

fn write_cols<W: Write>(
    writer: &mut BatchedWriter<W>,
    rec_ids: &mut Vec<u32>,
    values: &mut Vec<String>,
    val_name: &str,
) -> Result<usize> {
    let size = rec_ids.len();
    assert_eq!(values.len(), size);

    // turn record ids into a column - take ownership and swap out with new blank vector
    let mut rid_owned = Vec::with_capacity(BATCH_SIZE);
    mem::swap(&mut rid_owned, rec_ids);
    let id_col = Series::new("rec_id", &rid_owned);

    // create a value column
    let val_col = Series::new(val_name, &values);

    // make a batch
    let batch = DataFrame::new(vec![id_col, val_col])?;

    // and write
    writer.write_batch(&batch)?;

    Ok(size)
}

impl Command for FilterMARC {
    fn exec(&self) -> Result<()> {
        let (send, recv) = bounded(4096);
        let h = scan_records(self.field_file.as_path(), &self.filter, send)?;

        let nwritten = write_records(&self.output, recv)?;

        let nread = h.join().expect("thread join failed")?;
        info!(
            "wrote {} out of {} records",
            scalar(nwritten),
            scalar(nread)
        );

        Ok(())
    }
}
