/// Support code for Arrow RecordBatch IO.

use std::io::Write;
use anyhow::Result;

use arrow::record_batch::RecordBatch;
use arrow::csv::{Writer as CSVWriter};
use parquet::arrow::ArrowWriter;
use parquet::file::writer::ParquetWriter;

use crate::io::ObjectWriter;

impl <W: ParquetWriter + 'static> ObjectWriter<&RecordBatch> for ArrowWriter<W> {
  fn write_object(&mut self, object: &RecordBatch) -> Result<()> {
    self.write(object)?;
    Ok(())
  }

  fn finish(mut self) -> Result<usize> {
    self.close()?;
    Ok(0)
  }
}


impl <W: Write> ObjectWriter<&RecordBatch> for CSVWriter<W> {
  fn write_object(&mut self, object: &RecordBatch) -> Result<()> {
    self.write(object)?;
    Ok(())
  }

  fn finish(self) -> Result<usize> {
    Ok(0)
  }
}
