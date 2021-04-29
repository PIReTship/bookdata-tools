use anyhow::Result;

use crate::parquet::*;
use super::record::*;
use crate::io::ObjectWriter;
use crate as bookdata;  // hack to make derive macro work

/// Flat MARC field record.
#[derive(TableRow, Debug, Default)]
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
}

impl ObjectWriter<MARCRecord> for FieldOutput {
  fn write_object(&mut self, rec: &MARCRecord) -> Result<()> {
    self.rec_count += 1;
    let rec_id = self.rec_count;
    let mut fld_no = 0;

    // write the leader
    self.writer.write_object(&FieldRecord {
      rec_id, fld_no,
      tag: -1,
      ind1: 0, ind2: 0, sf_code: 0,
      contents: rec.leader.clone()
    })?;

    // write the control fields
    for cf in &rec.control {
      fld_no += 1;
      self.writer.write_object(&FieldRecord {
        rec_id, fld_no, tag: cf.tag.into(),
        ind1: 0, ind2: 0, sf_code: 0,
        contents: cf.content.clone()
      })?;
    }

    // write the data fields
    for df in &rec.fields {
      for sf in &df.subfields {
        fld_no += 1;
        self.writer.write_object(&FieldRecord {
          rec_id, fld_no,
          tag: df.tag, ind1: df.ind1, ind2: df.ind2,
          sf_code: sf.code,
          contents: sf.content.clone()
        })?;
      }
    }

    Ok(())
  }

  fn finish(self) -> Result<usize> {
    self.writer.finish()
  }
}
