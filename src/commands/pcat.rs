use structopt::StructOpt;

use std::io::prelude::*;
use std::io;
use std::fs::File;
use std::path::{PathBuf, Path};
use log::*;
use indicatif::{ProgressBar, ProgressStyle};
use sha1::Sha1;
use anyhow::Result;

use crate::db::{DbOpts, CopyRequest};
use crate::tracking::StageOpts;
use crate::io::{HashRead, HashWrite};
use crate::logging::set_progress;
use super::Command;

const PB_STYLE: &'static str = "{prefix}: {elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})";

/// Concatenate one or more files with a progress bar
#[derive(StructOpt, Debug)]
#[structopt(name="pcat")]
pub struct PCat {
  #[structopt(flatten)]
  dbo: DbOpts,

  #[structopt(flatten)]
  stage: StageOpts,

  /// Input file
  #[structopt(name = "FILE", parse(from_os_str))]
  infiles: Vec<PathBuf>,

  /// Destination table
  #[structopt(long="table", short="t")]
  table: Option<String>,

  /// Input format
  #[structopt(long="format", short="f")]
  format: Option<String>
}

/// Cat a file from input to output, hashing on the way.
fn cat_file<P: AsRef<Path>, W: Write>(inf: P, out: &mut W) -> Result<String> {
  let inf: &Path = inf.as_ref();
  let fstr = inf.to_string_lossy();
  info!("opening file {:?}", inf);
  let fs = File::open(inf)?;
  let pb = ProgressBar::new(fs.metadata().unwrap().len());
  pb.set_style(ProgressStyle::default_bar().template(PB_STYLE));
  pb.set_prefix(&fstr);
  let _pbs = set_progress(&pb);
  let mut hash = Sha1::new();
  let read = HashRead::create(fs, &mut hash);
  let mut pbr = pb.wrap_read(read);
  io::copy(&mut pbr, out)?;
  drop(pbr);
  Ok(hash.hexdigest())
}

impl Command for PCat {
  fn exec(self) -> Result<()> {
    match self.table {
      Some(ref t) => self.db_cat(t),
      None => self.raw_cat()
    }
  }
}

impl PCat {
  fn raw_cat(&self) -> Result<()> {
    let stdout = io::stdout();
    let mut out = stdout.lock();

    for inf in &self.infiles {
      cat_file(inf, &mut out)?;
    }
    Ok(())
  }

  fn db_cat(&self, table: &str) -> Result<()> {
    let db = self.dbo.open()?;
    self.stage.begin_stage(&db)?;
    let mut req = CopyRequest::new(&self.dbo, table)?.truncate(true);
    if let Some(ref fmt) = self.format {
      req = req.with_format(fmt);
    }
    info!("copying to table {}", table);
    let mut txout = self.stage.open_transcript()?;
    writeln!(&mut txout, "COPY TO {}", table)?;
    let out = req.open()?;
    let mut out_hash = Sha1::new();
    let mut out = HashWrite::create(out, &mut out_hash);

    for inf in &self.infiles {
      let inf = inf.as_path();
      let hash = cat_file(inf, &mut out)?;
      self.stage.record_file(&db, inf, &hash)?;
      writeln!(&mut txout, "READ {:?} {}", inf, hash)?;
    }

    drop(out);
    let hash = out_hash.hexdigest();
    self.stage.end_stage(&db, &Some(hash))?;
    Ok(())
  }
}
