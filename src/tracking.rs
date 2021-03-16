use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::fs::OpenOptions;

use anyhow::Result;
use sha1::Sha1;
use structopt::StructOpt;
use postgres::Connection;

use log::*;

use crate::io::HashRead;

/// Options controlling the import stage
#[derive(StructOpt, Debug, Clone)]
pub struct StageOpts {
  /// Stage name
  #[structopt(long="stage", short="s")]
  stage: Option<String>,

  /// Stage dependencies
  #[structopt(long="stage-dep", short="D")]
  deps: Vec<String>,

  /// Transcript file
  #[structopt(long="transcript", short="T")]
  transcript: Option<PathBuf>,
}

/// An import stage.  Writing to the stage writes to its transcript file.
pub struct Stage<'o, 'c> {
  options: &'o StageOpts,
  cxn: Option<&'c Connection>,
  transcript: Box<dyn io::Write>
}

/// A source file for a stage
pub struct StageSource<'s> {
  stage: &'s Stage<'s, 's>,
  path: String,
  hash: Sha1
}

impl StageOpts {
  /// Start the stage
  pub fn begin_stage<'o, 'c>(&'o self, cxn: &'c Connection) -> Result<Stage<'o, 'c>> {
    match self.stage {
      Some (ref s) => {
        info!("beginning stage {}", s);
        cxn.execute("INSERT INTO stage_status (stage_name)
                     VALUES ($1)
                     ON CONFLICT (stage_name)
                     DO UPDATE SET started_at = now(), finished_at = NULL, stage_key = NULL",
                    &[s])?;
        cxn.execute("DELETE FROM stage_file WHERE stage_name = $1", &[s])?;
        cxn.execute("DELETE FROM stage_dep WHERE stage_name = $1", &[s])?;
        for d in &self.deps {
          cxn.execute("INSERT INTO stage_dep (stage_name, dep_name, dep_key)
                       SELECT $1, stage_name, stage_key
                       FROM stage_status WHERE stage_name = $2", &[s, &d])?;
        }
      },
      None => {
        warn!("no stage specified");
      }
    };
    let w: Box<dyn io::Write> = match self.transcript {
      Some(ref p) => {
        Box::new(OpenOptions::new().write(true).create(true).truncate(true).open(p)?)
      },
      None => Box::new(io::stdout())
    };
    Ok(Stage {
      options: self,
      cxn: Some(cxn),
      transcript: w
    })
  }

  /// Create a no-op stage.
  pub fn empty<'o>(&'o self) -> Stage<'o, 'o> {
    Stage {
      options: self,
      cxn: None,
      transcript: Box::new(io::stderr())
    }
  }
}

impl <'o, 'c> Write for Stage<'o, 'c> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.transcript.write(buf)
  }

  fn flush(&mut self) -> io::Result<()> {
    self.transcript.flush()
  }

  fn write_fmt(&mut self, fmt: std::fmt::Arguments) -> io::Result<()> {
    self.transcript.write_fmt(fmt)
  }
}

impl <'o,'c> Stage<'o,'c> {
  /// End the stage
  pub fn end(self, key: &Option<String>) -> Result<()> {
    match self.options.stage {
      Some (ref s) => {
        info!("finishing stage {}", s);
        self.db_action(|db| {
          Ok(db.execute("UPDATE stage_status
                         SET finished_at = NOW(), stage_key = $2
                         WHERE stage_name = $1",
                        &[s, &key])?)
        })?;
      },
      None => ()
    };
    Ok(())
  }

  fn db_action<F, R, Rt>(&self, func: F) -> Result<Option<R>> where F: FnOnce(&Connection) -> Rt, Rt: Into<Result<R>> {
    match self.cxn {
      Some(ref c) => Ok(Some(func(c).into()?)),
      None => Ok(None)
    }
  }

  /// Set up to record a file with its reader, to both source and transcript
  pub fn source_file<'s, P: AsRef<Path>>(&'s self, path: P) -> StageSource<'s> {
    let path: &Path = path.as_ref();
    StageSource {
      stage: self,
      path: path.to_string_lossy().to_string(),
      hash: Sha1::new()
    }
  }
}

impl <'s> StageSource<'s> {
  /// Wrap a reader to compute this file's hash
  pub fn wrap_read<'a, R: io::Read>(&'a mut self, read: R) -> HashRead<'a, R> {
    HashRead::create(read, &mut self.hash)
  }

  /// Record the accumulated file hash (and return it)
  pub fn record(self) -> Result<String> {
    let hash = self.hash.hexdigest();
    self.record_hash(&hash)?;
    Ok(hash)
  }

  fn record_hash(&self, hash: &str) -> Result<()> {
    info!("recording checksum {} for file {}", hash, &self.path);
    self.stage.db_action(|db| {
      db.execute("INSERT INTO source_file (filename, checksum)
                  VALUES ($1, $2)
                  ON CONFLICT (filename)
                  DO UPDATE SET checksum = $2, reg_time = NOW()",
                 &[&self.path, &hash])?;
      match self.stage.options.stage {
        Some (ref s) => {
          debug!("attaching to stage {}", s);
          db.execute("INSERT INTO stage_file (stage_name, filename)
                      VALUES ($1, $2)",
                     &[s, &self.path])?
        },
        None => 0
      };
      Ok(())
    })?;
    Ok(())
  }
}
