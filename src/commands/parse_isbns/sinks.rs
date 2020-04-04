use std::io::prelude::*;
use anyhow::Result;

use super::parsers::ISBN;

pub trait WriteISBNs {
  fn write_isbn(&mut self, id: i64, isbn: &ISBN) -> Result<()>;
}

pub struct NullWriter {}

impl WriteISBNs for NullWriter {
  fn write_isbn(&mut self, _id: i64, _isbn: &ISBN) -> Result<()> {
    Ok(())
  }
}

pub struct FileWriter {
  pub write: Box<dyn Write>
}

impl WriteISBNs for FileWriter {
  fn write_isbn(&mut self, id: i64, isbn: &ISBN) -> Result<()> {
    writeln!(self.write, "{}\t{}\t{}", id, isbn.text,
             isbn.descriptor.as_ref().map(|s| s.as_str()).unwrap_or(""))?;
    Ok(())
  }
}
