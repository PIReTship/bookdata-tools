use std::io;
use std::path::{Path, PathBuf};
use std::fs::OpenOptions;

use anyhow::Result;
use structopt::StructOpt;
use postgres::Connection;

use log::*;

/// An import stage
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

impl StageOpts {
  /// Start the stage
  pub fn begin_stage(&self, cxn: &Connection) -> Result<()> {
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
    Ok(())
  }

  /// End the stage
  pub fn end_stage(&self, cxn: &Connection, key: &Option<String>) -> Result<()> {
    match self.stage {
      Some (ref s) => {
        info!("finishing stage {}", s);
        cxn.execute("UPDATE stage_status
                     SET finished_at = NOW(), stage_key = $2
                     WHERE stage_name = $1",
                    &[s, &key])?
      },
      None => 0
    };
    Ok(())
  }

  /// Record a file
  pub fn record_file<P: AsRef<Path>>(&self, cxn: &Connection, path: P, hash: &str) -> Result<()> {
    let path: &Path = path.as_ref();
    let name = path.to_string_lossy();
    info!("recording checksum {} for file {}", hash, name);
    cxn.execute("INSERT INTO source_file (filename, checksum)
                 VALUES ($1, $2)
                 ON CONFLICT (filename)
                 DO UPDATE SET checksum = $2, reg_time = NOW()",
                &[&name, &hash])?;
    match self.stage {
      Some (ref s) => {
        debug!("attaching to stage {}", s);
        cxn.execute("INSERT INTO stage_file (stage_name, filename)
                     VALUES ($1, $2)",
                    &[s, &name])?
      },
      None => 0
    };
    Ok(())
  }

  pub fn open_transcript(&self) -> Result<Box<dyn io::Write>> {
    let w: Box<dyn io::Write> = match self.transcript {
      Some(ref p) => {
        Box::new(OpenOptions::new().write(true).create(true).truncate(true).open(p)?)
      },
      None => Box::new(io::stdout())
    };
    Ok(w)
  }
}
