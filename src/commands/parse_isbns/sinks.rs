use std::io::prelude::*;
use anyhow::Result;
use sha1::Sha1;

use crate::cleaning::isbns::ISBN;
use crate::db::{ConnectInfo, CopyRequest, CopyTarget};
use crate::cleaning::write_pgencoded;

pub trait WriteISBNs {
  fn write_isbn(&mut self, id: i64, isbn: &ISBN) -> Result<()>;

  fn finish(&mut self) -> Result<String> {
    Ok("".to_owned())
  }
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

pub struct DBWriter {
  hash: Sha1,
  target: CopyTarget
}

impl DBWriter {
  pub fn new<C: ConnectInfo>(db: &C, table: &str) -> Result<DBWriter> {
    let req = CopyRequest::new(db, table)?;
    let req = req.truncate(true);

    Ok(DBWriter {
      hash: Sha1::new(),
      target: req.open()?
    })
  }
}

impl WriteISBNs for DBWriter {
  fn write_isbn(&mut self, id: i64, isbn: &ISBN) -> Result<()> {
    self.hash.update(id.to_string().as_bytes());
    self.hash.update(isbn.text.as_bytes());
    if isbn.tags.len() > 0 {
      for tag in &isbn.tags {
        self.hash.update(tag.as_bytes());
        write!(self.target, "{}\t{}\t", id, isbn.text)?;
        write_pgencoded(&mut self.target, tag.as_bytes())?;
        writeln!(self.target)?;
      }
    } else {
      writeln!(self.target, "{}\t{}\t", id, isbn.text)?;
    }
    Ok(())
  }

  fn finish(&mut self) -> Result<String> {
    let hash = self.hash.hexdigest();
    Ok(hash.to_owned())
  }
}
