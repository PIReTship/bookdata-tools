//! Command to filter MARC output.
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::StringBuilder;
use arrow::array::UInt32Builder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use friendly::scalar;
use parquet::arrow::ArrowWriter;

use crate::arrow::scan_parquet_file;
use crate::arrow::writer::parquet_writer_defaults;
use crate::io::object::UnchunkWriter;
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

struct FilterOutput<W: ObjectWriter<RecordBatch>> {
    schema: Arc<Schema>,
    writer: W,
}

impl<W: ObjectWriter<RecordBatch>> ObjectWriter<Vec<FieldRecord>> for FilterOutput<W> {
    fn write_object(&mut self, object: Vec<FieldRecord>) -> Result<()> {
        let size = object.len();

        let mut id_col = UInt32Builder::with_capacity(size);
        let mut val_col = StringBuilder::with_capacity(size, size * 10);

        for rec in object {
            id_col.append_value(rec.rec_id);
            val_col.append_value(rec.contents);
        }

        let id_col = id_col.finish();
        let val_col = val_col.finish();
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(id_col), Arc::new(val_col)],
        )?;

        self.writer.write_object(batch)?;
        Ok(())
    }

    fn finish(self) -> Result<usize> {
        self.writer.finish()
    }
}

/// Scan MARC records from a file.
///
/// Failes quickly if there is an error opening the file; errors reading the file are
/// from the thread and are availabl when it is joined.
fn scan_records(
    path: &Path,
    filter: &FilterSpec,
    out: impl ObjectWriter<FieldRecord> + Send,
) -> Result<(usize, usize)> {
    info!("reading names from authority fields in {:?}", path);
    let scanner = scan_parquet_file(path)?;
    let mut out = out;

    let scanner = scanner;
    let mut nr = 0;
    let mut nw = 0;
    for rec in scanner {
        nr += 1;
        let mut rec: FieldRecord = rec?;
        if filter.matches(&rec) {
            nw += 1;
            rec.contents = filter.transform(rec.contents.as_str()).into();
            out.write_object(rec)?;
        }
    }
    debug!("finished scanning parquet");
    out.finish()?;
    Ok((nr, nw))
}

/// Create an output for the records.
fn write_records(out: &OutputSpec) -> Result<impl ObjectWriter<FieldRecord> + Send> {
    info!("writing output to {:?}", out.file);
    let out_name = out
        .content_name
        .as_ref()
        .map(|s| s.clone())
        .unwrap_or("content".into());
    let schema = Schema::new(vec![
        Field::new("rec_id", DataType::UInt32, false),
        Field::new(&out_name, DataType::Utf8, false),
    ]);
    let schema = Arc::new(schema);

    let file = File::options()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&out.file)?;
    let props = parquet_writer_defaults().set_column_dictionary_enabled(out_name.into(), true);
    let writer = ArrowWriter::try_new(file, schema.clone(), Some(props.build()))?;
    let writer = FilterOutput { schema, writer };
    let writer = UnchunkWriter::with_size(writer, BATCH_SIZE);

    Ok(writer)
}

impl Command for FilterMARC {
    fn exec(&self) -> Result<()> {
        let out = write_records(&self.output)?;
        let (nr, nw) = scan_records(self.field_file.as_path(), &self.filter, out)?;

        info!("wrote {} out of {} records", scalar(nw), scalar(nr));

        Ok(())
    }
}
