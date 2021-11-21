use std::path::Path;

use crate::prelude::*;
use crate::arrow::*;
use crate::marc::flat_fields::FieldOutput;

pub fn open_output<P: AsRef<Path>>(path: P) -> Result<FieldOutput> {
  let w = TableWriter::open(path)?;
  Ok(FieldOutput::new(w))
}
