use std::io::prelude::*;
use std::path::{Path,PathBuf};
use std::fs::File;
use std::io::{BufReader, Lines};
use std::mem::drop;

use anyhow::{Result, anyhow};

use regex::{Regex, Captures};
use postgres::Connection;
use postgres::rows::LazyRows;

use log::*;
use structopt::{StructOpt};
use fallible_iterator::FallibleIterator;

use super::Command;
use crate::db::DbOpts;
use crate::tsv::split_first;

/// Parse MARC files into records for a PostgreSQL table.
#[derive(StructOpt, Debug)]
#[structopt(name="parse-isbns")]
pub struct ParseISBNs {
  #[structopt(flatten)]
  db: DbOpts,

  /// The table from which to parse ISBNs.
  #[structopt(short="-s", long="src-table")]
  src_table: Option<String>,

  /// The file from which to parse ISBNs.
  #[structopt(short="-f", long="src-file")]
  src_file: Option<PathBuf>,

  /// Print unmatched entries
  #[structopt(short="-U", long="print-unmatched")]
  print_unmatched: bool,

  /// Print ignored entries
  #[structopt(short="-I", long="print-ignored")]
  print_ignored: bool
}

#[derive(Debug)]
struct ISBN {
  text: String,
  descriptor: Option<String>
}

fn parse_plain<'t>(m: &Captures<'t>) -> ISBN {
  let isbn = m.get(1).unwrap().as_str();
  let desc = m.get(2).map(|m| m.as_str().to_owned());
  ISBN {
    text: isbn.to_owned(),
    descriptor: desc
  }
}

static PARSERS: &'static [(&'static str, fn(&Captures) -> ISBN)] = &[
  ("^(?:[a-z][[:space:]]+|\\([[:digit:]]+\\)[[:space:]]+|\\*)?([0-9 -][Xx]?)(?:[[:space:]]*\\((.+?)\\))?",
   parse_plain)
];

static IGNORES: &'static [&'static str] = &[
  "^[$£][[:digit:]., ]+$"
];

struct IsbnParser {
  parsers: Vec<(Regex, &'static fn(&Captures) -> ISBN)>,
  ignores: Vec<Regex>
}

#[derive(Debug)]
enum ParseResult {
  Valid(ISBN),
  Ignored(String),
  Unmatched(String)
}

impl IsbnParser {
  fn compile() -> Result<IsbnParser> {
    let mut compiled = Vec::with_capacity(PARSERS.len());
    for (pat, func) in PARSERS {
      let rex = Regex::new(pat)?;
      compiled.push((rex, func));
    }

    let mut comp_ignore = Vec::with_capacity(IGNORES.len());
    for pat in IGNORES {
      comp_ignore.push(Regex::new(pat)?);
    }

    Ok(IsbnParser {
      parsers: compiled,
      ignores: comp_ignore
    })
  }

  fn parse<'a>(&self, text: &'a str) -> ParseResult {
    for (rex, func) in &self.parsers {
      if let Some(cap) = rex.captures(text) {
        return ParseResult::Valid(func(&cap))
      }
    }
    for rex in &self.ignores {
      if rex.is_match(text) {
        return ParseResult::Ignored(text.to_owned())
      }
    }
    ParseResult::Unmatched(text.to_owned())
  }
}

type IdPR = (i64, ParseResult);

struct FileSource<B> {
  parsers: IsbnParser,
  lines: Lines<B>
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

struct DBSource<'t, 's> {
  parsers: IsbnParser,
  rows: LazyRows<'t, 's>
}

impl <'t, 's> DBSource<'t, 's> {
  fn create(rows: LazyRows<'t, 's>) -> Result<DBSource<'t, 's>> {
    Ok(DBSource {
      parsers: IsbnParser::compile()?,
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


struct MatchStats {
  total: u64,
  valid: u64,
  ignored: u64,
  unmatched: u64
}

impl Default for MatchStats {
  fn default() -> MatchStats {
    MatchStats {
      total: 0,
      valid: 0,
      ignored: 0,
      unmatched: 0
    }
  }
}


impl ParseISBNs {
  fn scan_source<R>(&self, iter: &mut R) -> Result<MatchStats>
      where R: FallibleIterator<Item = IdPR, Error = anyhow::Error> {
    let mut stats = MatchStats::default();
    while let Some((id, result)) = iter.next()? {
      debug!("{}: {:?}", id, result);
      match result {
        ParseResult::Valid(isbn) => {
          stats.valid += 1;
        },
        ParseResult::Ignored (s)=> {
          stats.ignored += 1;
          if self.print_ignored {
            println!("ignored {}: {}", id, s)
          }
        },
        ParseResult::Unmatched(s) => {
          stats.unmatched += 1;
          if self.print_unmatched {
            println!("unmatched {}: {}", id, s);
          }
        }
      }
      stats.total += 1;
    }
    Ok(stats)
  }

  fn scan_file(&self, file: &Path) -> Result<MatchStats> {
    let parsers = IsbnParser::compile()?;
    let input = File::open(file)?;
    let input = BufReader::new(input);
    let mut src = FileSource {
      parsers: parsers,
      lines: input.lines()
    };
    self.scan_source(&mut src)
  }

  fn scan_db(&self, db: &Connection, table: &str) -> Result<MatchStats> {
    let query = format!("SELECT * FROM {}", table);
    let txn = db.transaction()?;
    let stmt = txn.prepare(&query)?;
    let rows = stmt.lazy_query(&txn, &[], 1000)?;
    let mut src = DBSource::create(rows)?;
    let stats = self.scan_source(&mut src)?;
    drop(src);
    drop(stmt);
    txn.commit()?;
    Ok(stats)
  }
}

impl Command for ParseISBNs {
  fn exec(self) -> Result<()> {
    let stats = if let Some(ref tbl) = self.src_table {
      let db = self.db.open()?;
      let n = self.scan_db(&db, tbl)?;
      n
    } else if let Some(ref path) = self.src_file {
      self.scan_file(&path)?
    } else {
      error!("no source data specified");
      return Err(anyhow!("no source data"));
    };
    info!("processed {} ISBN records", stats.total);
    info!("matched {}, ignored {}, and {} were unmatched",
          stats.valid, stats.ignored, stats.unmatched);
    Ok(())
  }
}
