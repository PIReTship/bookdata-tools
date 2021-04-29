use std::io::prelude::*;
use std::io::Lines;
use anyhow::Result;

use postgres::rows::LazyRows;
use fallible_iterator::FallibleIterator;

use log::*;

use crate::tsv::split_first;
use crate::ids::isbn::*;

pub type IdPR = (i64, ParseResult);

pub struct FileSource<B> {
  parsers: ParserDefs,
  lines: Lines<B>
}

impl <B: BufRead> FileSource<B> {
  pub fn create(read: B) -> Result<FileSource<B>> {
    Ok(FileSource {
      parsers: ParserDefs::new(),
      lines: read.lines()
    })
  }
}

impl <B: BufRead> FallibleIterator for FileSource<B> {
  type Item = IdPR;
  type Error = anyhow::Error;

  fn next(&mut self) -> Result<Option<IdPR>> {
    let nl = self.lines.next();
    match nl {
      None => Ok(None),
      Some(line) => {
        let text = line?;
        let (id, isbn) = split_first(&text).unwrap();
        Ok(Some((id.parse::<i64>()?, self.parsers.parse(isbn))))
      }
    }
  }
}

pub struct DBSource<'t, 's> {
  parsers: ParserDefs,
  rows: LazyRows<'t, 's>
}

impl <'t, 's> DBSource<'t, 's> {
  pub fn create(rows: LazyRows<'t, 's>) -> Result<DBSource<'t, 's>> {
    Ok(DBSource {
      parsers: ParserDefs::new(),
      rows: rows
    })
  }
}

impl <'t, 's> FallibleIterator for DBSource<'t, 's> {
  type Item = IdPR;
  type Error = anyhow::Error;

  fn next(&mut self) -> Result<Option<IdPR>> {
    if let Some(row) = self.rows.next()? {
      let id: i32 = row.get(0);
      let content: String = row.get(1);
      let result = self.parsers.parse(&content);
      debug!("{}: {:?}", id, result);
      Ok(Some((id.into(), result)))
    } else {
      Ok(None)
    }
  }
}
