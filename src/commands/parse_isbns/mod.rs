use std::io::prelude::*;
use std::path::{Path,PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::mem::drop;

use anyhow::{Result, anyhow};

use postgres::Connection;

use log::*;
use structopt::{StructOpt};
use fallible_iterator::FallibleIterator;

use super::Command;
use crate::db::DbOpts;
use crate::tracking::{StageOpts};

mod sources;
mod sinks;

use crate::ids::isbn::*;
use sources::*;
use sinks::*;

/// Parse MARC files into records for a PostgreSQL table.
#[derive(StructOpt, Debug)]
#[structopt(name="parse-isbns")]
pub struct ParseISBNs {
  #[structopt(flatten)]
  db: DbOpts,

  #[structopt(flatten)]
  stage: StageOpts,

  /// The table from which to parse ISBNs.
  #[structopt(long="src-table")]
  src_table: Option<String>,

  /// The file from which to parse ISBNs.
  #[structopt(short="-f", long="src-file")]
  src_file: Option<PathBuf>,

  // The file to write the parsed ISBNs.
  #[structopt(short="-o", long="out-file")]
  out_file: Option<PathBuf>,

  // The table to write the parsed ISBNs.
  #[structopt(long="out-table")]
  out_table: Option<String>,

  /// Print unmatched entries
  #[structopt(short="-U", long="print-unmatched")]
  print_unmatched: bool,

  /// Print ignored entries
  #[structopt(short="-I", long="print-ignored")]
  print_ignored: bool,

  /// Print trails
  #[structopt(short="-L", long="print-trail")]
  print_trail: bool,

  /// Skip ISBN diagnostics
  #[structopt(long="no-diagnostics")]
  skip_diagnostics: bool
}

struct MatchStats {
  total: usize,
  valid: usize,
  ignored: usize,
  unmatched: usize,
  hash: Option<String>
}

impl Default for MatchStats {
  fn default() -> MatchStats {
    MatchStats {
      total: 0,
      valid: 0,
      ignored: 0,
      unmatched: 0,
      hash: None
    }
  }
}

impl ParseISBNs {
  fn scan_source<R>(&self, iter: &mut R, writer: Box<dyn WriteISBNs>) -> Result<MatchStats>
      where R: FallibleIterator<Item = IdPR, Error = anyhow::Error> {
        let mut stats = MatchStats::default();
    let mut w = writer;  // we need a mutable writer
    while let Some((id, result)) = iter.next()? {
      debug!("{}: {:?}", id, result);
      match result {
        ParseResult::Valid(isbns, trail) => {
          for isbn in &isbns {
            self.check_isbn(id, isbn, &trail);
            w.write_isbn(id, isbn)?;
          }
          let trail = trail.trim();
          if trail.len() > 0 && self.print_trail {
            println!("trail for {}: {}", id, trail);
          }
          stats.valid += isbns.len();
        },
        ParseResult::Ignored (s)=> {
          stats.ignored += 1;
          if self.print_ignored {
            println!("ignored {}: {}", id, s);
          }
        },
        ParseResult::Unmatched(s) => {
          stats.unmatched += 1;
          warn!("unmatched {}: {}", id, s);
          if self.print_unmatched {
            println!("unmatched {}: {}", id, s);
          }
        }
      }
      stats.total += 1;
    }
    let hash = w.finish()?;
    if hash.len() > 0 {
      stats.hash = Some(hash);
    }
    Ok(stats)
  }

  fn check_isbn(&self, id: i64, isbn: &ISBN, trail: &str) {
    if self.skip_diagnostics {
      return;
    }
    if isbn.text.len() > 15 {
      warn!("{}: isbn {} {} chars long", id, isbn.text, isbn.text.len());
    }
    if isbn.text.len() < 8 {
      warn!("{}: isbn {} shorter than 8 characters (trail: ‘{}’)", id, isbn.text, trail);
    }
  }

  fn scan_file(&self, file: &Path, writer: Box<dyn WriteISBNs>) -> Result<MatchStats> {
    let input = File::open(file)?;
    let input = BufReader::new(input);
    let mut src = FileSource::create(input)?;
    self.scan_source(&mut src, writer)
  }

  fn scan_db(&self, db: &Connection, table: &str, writer: Box<dyn WriteISBNs>) -> Result<MatchStats> {
    let query = format!("SELECT * FROM {}", table);
    let txn = db.transaction()?;
    let stmt = txn.prepare(&query)?;
    let rows = stmt.lazy_query(&txn, &[], 1000)?;
    let mut src = DBSource::create(rows)?;
    let stats = self.scan_source(&mut src, writer)?;
    drop(src);
    drop(stmt);
    txn.commit()?;
    Ok(stats)
  }
}

impl Command for ParseISBNs {
  fn exec(self) -> Result<()> {
    let db = self.db.open()?;
    let mut stage = self.stage.begin_stage(&db)?;
    let writer: Box<dyn WriteISBNs> = if let Some(ref tbl) = self.out_table {
      info!("opening output table {}", tbl);
      writeln!(stage, "DEST TABLE {}", tbl)?;
      Box::new(DBWriter::new(&self.db, tbl)?)
    } else if let Some(ref path) = self.out_file {
      info!("opening output file {:?}", path);
      writeln!(stage, "DEST FILE {:?}", path)?;
      let out = OpenOptions::new().write(true).create(true).truncate(true).open(path)?;
      let buf = BufWriter::new(out);
      Box::new(FileWriter {
        write: Box::new(buf)
      })
    } else {
      Box::new(NullWriter {})
    };
    let stats = if let Some(ref tbl) = self.src_table {
      writeln!(stage, "SOURCE TABLE {}", tbl)?;
      let n = self.scan_db(&db, tbl, writer)?;
      n
    } else if let Some(ref path) = self.src_file {
      writeln!(stage, "SOURCE FILE {:?}", path)?;
      self.scan_file(&path, writer)?
    } else {
      error!("no source data specified");
      return Err(anyhow!("no source data"));
    };
    writeln!(stage, "{} RECORDS", stats.total)?;
    writeln!(stage, "{} IMPORTED", stats.valid)?;
    writeln!(stage, "{} UNMATCHED", stats.unmatched)?;
    writeln!(stage, "{} IGNORED", stats.ignored)?;
    if let Some(ref h) = stats.hash {
      writeln!(stage, "OUT HASH {}", h)?;
    }
    stage.end(&stats.hash)?;
    info!("processed {} ISBN records", stats.total);
    info!("matched {}, ignored {}, and {} were unmatched",
          stats.valid, stats.ignored, stats.unmatched);
    Ok(())
  }
}
