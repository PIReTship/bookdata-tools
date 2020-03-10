use structopt::StructOpt;

use std::io;
use std::fs::File;
use std::path::PathBuf;
use std::mem::drop;
use log::*;
use indicatif::{ProgressBar, ProgressStyle};
use postgres::Connection;
use sha1::Sha1;
use anyhow::Result;

use super::Command;
use crate::db;

const PB_STYLE: &'static str = "{prefix}: {elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})";

/// Concatenate one or more files with a progress bar
#[derive(StructOpt, Debug)]
#[structopt(name="hash")]
pub struct Hash {
  #[structopt(flatten)]
  db: db::DbOpts,

  /// Input file
  #[structopt(name = "FILE", parse(from_os_str))]
  infiles: Vec<PathBuf>
}

trait Hasher: io::Write {
  fn finish(self) -> String;
}

struct H {
  sha: Sha1
}

impl Hasher for H {
  fn finish(self) -> String {
    self.sha.hexdigest()
  }
}

impl io::Write for H {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.sha.update(buf);
    Ok(buf.len())
  }

  fn flush(&mut self) -> io::Result<()> {
    Ok(())
  }
}

fn save_hash(db: &mut Connection, file: &str, hash: &str) -> Result<()> {
  info!("saving {}: {}", file, hash);
  let tx = db.transaction()?;
  tx.execute("DELETE FROM source_file WHERE filename = $1", &[&file])?;
  tx.execute("INSERT INTO source_file (filename, checksum) VALUES ($1, $2)", &[&file, &hash])?;
  tx.commit()?;
  Ok(())
}

impl Command for Hash {
  fn exec(self) -> Result<()> {
    let mut db = self.db.open()?;

    for inf in self.infiles {
      let inf = inf.as_path();
      let fstr = inf.to_str().unwrap();
      info!("opening file {}", fstr);
      let fs = File::open(inf)?;
      let pb = ProgressBar::new(fs.metadata().unwrap().len());
      pb.set_style(ProgressStyle::default_bar().template(PB_STYLE));
      pb.set_prefix(fstr);
      let mut pbr = pb.wrap_read(fs);
      let mut hash = H { sha: Sha1::new() };
      io::copy(&mut pbr, &mut hash)?;
      drop(pbr);
      let hash = hash.finish();
      save_hash(&mut db, &fstr, &hash)?;
    }

    Ok(())
  }
}
