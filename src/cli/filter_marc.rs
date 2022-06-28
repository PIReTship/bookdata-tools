//! Command to filter MARC output.
use std::path::{PathBuf, Path};
use std::io::Write;
use std::fs::File;
use std::mem;
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use std::thread::{spawn, JoinHandle};

use arrow2::array::{MutablePrimitiveArray, MutableUtf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Schema, DataType, Field};
use arrow2::io::parquet::write::{FileWriter, WriteOptions, Version, CompressionOptions};
use friendly::scalar;
use structopt::StructOpt;

use crate::marc::flat_fields::FieldRecord;
use crate::prelude::*;
use crate::arrow::*;
use crate::util::logging::{set_progress, item_progress};

const BATCH_SIZE: usize = 1024 * 1024;

/// Filter a MARC field file to only contain certain results.
#[derive(StructOpt, Debug)]
#[structopt(name="filter-marc")]
pub struct FilterMARC {
  #[structopt(flatten)]
  filter: FilterSpec,

  #[structopt(flatten)]
  output: OutputSpec,

  /// Input file of MARC field data.
  #[structopt(name="FIELD_FILE", parse(from_os_str))]
  field_file: PathBuf,
}

/// Options for filtering MARC records.
#[derive(StructOpt, Debug, Clone)]
struct FilterSpec {
  /// Specify the tag to filter to.
  #[structopt(short="t", long="tag", name="TAG")]
  tag: Option<i16>,

  /// Specify the subfield to filter to.
  #[structopt(short="f", long="subfield", name="CODE")]
  subfield: Option<char>,

  /// Trim the contents before emitting.
  #[structopt(short="T", long="trim")]
  trim: bool,

  /// Lowercase the contents before emitting.
  #[structopt(short="L", long="lower")]
  lower: bool,
}

/// Options for output.
#[derive(StructOpt, Debug, Clone)]
struct OutputSpec {
  /// Rename the content field.
  #[structopt(short="n", long="name", name="FIELD")]
  content_name: Option<String>,

  /// Output file for filtered MARC fields.
  #[structopt(short="o", long="output", name = "FILE", parse(from_os_str))]
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
fn scan_records(path: &Path, filter: &FilterSpec, send: SyncSender<FieldRecord>) -> Result<JoinHandle<Result<usize>>> {
  info!("reading names from authority fields in {:?}", path);
  let scanner = scan_parquet_file(path)?;
  let filter = filter.clone(); // to transfer to thread

  Ok(spawn(move || {
    let scanner = scanner;
    let pb = item_progress(scanner.remaining(), "outer");
    let _lg = set_progress(pb.clone());
    let mut n = 0;
    for rec in pb.wrap_iter(scanner) {
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
  let schema = Schema {
    fields: vec![
      Field {
        name: "rec_id".into(),
        data_type: DataType::UInt32,
        is_nullable: false,
        metadata: Default::default(),
      },
      Field {
        name: out.content_name.as_ref().map(|s| s.clone()).unwrap_or("content".into()),
        data_type: DataType::UInt32,
        is_nullable: false,
        metadata: Default::default(),
      },
    ],
    metadata: Default::default()
  };

  let writer = File::create(&out.file)?;
  let options = WriteOptions {
    compression: CompressionOptions::Zstd(None),
    version: Version::V2,
    write_statistics: false,
  };
  let mut writer = FileWriter::try_new(writer, schema, options)?;

  let mut rec_ids = Vec::with_capacity(BATCH_SIZE);
  let mut values = Vec::with_capacity(BATCH_SIZE);
  let mut n = 0;

  for rec in recv {
    rec_ids.push(rec.rec_id);
    values.push(rec.contents);
    if rec_ids.len() >= BATCH_SIZE {
      n += write_cols(&mut writer, &mut rec_ids, &mut values)?;
    }
  }

  n += write_cols(&mut writer, &mut rec_ids, &mut values)?;

  writer.finish()?;

  Ok(n)
}

fn write_cols<W: Write>(writer: &mut FileWriter<W>, rec_ids: &mut Vec<u32>, values: &mut Vec<String>) -> Result<usize> {
  let size = rec_ids.len();
  assert_eq!(values.len(), size);

  // turn record ids into a column - take ownership and swap out with new blank vector
  let mut rid_owned = Vec::with_capacity(BATCH_SIZE);
  mem::swap(&mut rid_owned, rec_ids);
  let rec_col = MutablePrimitiveArray::from_vec(rid_owned);

  // create a value column
  let mut val_col = MutableUtf8Array::<i32>::with_capacity(values.len());
  val_col.extend_values(values.iter());
  values.clear();

  // make a chunk
  let rec_col = rec_col.into_arc();
  let val_col = val_col.into_arc();
  let chunk = Chunk::new(vec![rec_col, val_col]);

  // and write
  writer.write_object(chunk)?;

  Ok(size)
}

impl Command for FilterMARC {
  fn exec(&self) -> Result<()> {
    let (send, recv) = sync_channel(4096);
    let h = scan_records(self.field_file.as_path(), &self.filter, send)?;

    let nwritten = write_records(&self.output, recv)?;

    let nread = h.join().expect("thread join failed")?;
    info!("wrote {} out of {} records", scalar(nwritten), scalar(nread));

    Ok(())
  }
}
