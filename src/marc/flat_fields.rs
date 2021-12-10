use std::path::Path;
use anyhow::Result;

use crate::arrow::*;
use super::record::*;
use crate::io::*;

/// Flat MARC field record.
#[derive(ParquetRecordWriter, Debug, Default)]
pub struct FieldRecord {
  rec_id: u32,
  fld_no: u32,
  tag: i16,
  ind1: u8,
  ind2: u8,
  sf_code: u8,
  contents: String,
}

/// Output for writing flat MARC fields to Parquet.
pub struct FieldOutput {
  rec_count: u32,
  writer: TableWriter<FieldRecord>
}

impl FieldOutput {
  /// Create a new output.
  pub fn new(writer: TableWriter<FieldRecord>) -> FieldOutput {
    FieldOutput {
      rec_count: 0,
      writer
    }
  }

  /// Open a field output going to a file.
  pub fn open<P: AsRef<Path>>(path: P) -> Result<FieldOutput> {
    let writer = TableWriter::open(path)?;
    Ok(Self::new(writer))
  }
}

impl DataSink for FieldOutput {
  fn output_files(&self) -> Vec<std::path::PathBuf> {
    self.writer.output_files()
  }
}

impl ObjectWriter<MARCRecord> for FieldOutput {
  fn write_object(&mut self, rec: MARCRecord) -> Result<()> {
    self.rec_count += 1;
    let rec_id = self.rec_count;
    let mut fld_no = 0;

    // write the leader
    self.writer.write_object(FieldRecord {
      rec_id, fld_no,
      tag: -1,
      ind1: 0.into(), ind2: 0.into(), sf_code: 0.into(),
      contents: rec.leader
    })?;

    // write the control fields
    for cf in rec.control {
      fld_no += 1;
      self.writer.write_object(FieldRecord {
        rec_id, fld_no, tag: cf.tag.into(),
        ind1: 0.into(), ind2: 0.into(), sf_code: 0.into(),
        contents: cf.content
      })?;
    }

    // write the data fields
    for df in rec.fields {
      for sf in df.subfields {
        fld_no += 1;
        self.writer.write_object(FieldRecord {
          rec_id, fld_no,
          tag: df.tag, ind1: df.ind1.into(), ind2: df.ind2.into(),
          sf_code: sf.code.into(),
          contents: sf.content
        })?;
      }
    }

    Ok(())
  }

  fn finish(self) -> Result<usize> {
    self.writer.finish()
  }
}
