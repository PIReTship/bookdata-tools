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
    if isbn.tags.len() > 0 {
      for tag in &isbn.tags {
        writeln!(self.write, "{}\t{}\t{}", id, isbn.text, tag)?;
      }
    } else {
      writeln!(self.write, "{}\t{}\t", id, isbn.text)?;
    }
    Ok(())
  }
}
