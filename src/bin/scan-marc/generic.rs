use std::path::Path;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::marc::flat_fields::FieldOutput;

pub fn open_output<P: AsRef<Path>>(path: P) -> Result<FieldOutput> {
  let w = TableWriter::open(path)?;
  Ok(FieldOutput::new(w))
}
