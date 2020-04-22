use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::fs::{File, read_to_string};
use std::path::PathBuf;

use log::*;

use structopt::StructOpt;
use flate2::bufread::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use sha1::Sha1;
use anyhow::{Result};
use serde::{Deserialize};
use toml;

use crate::io::{HashRead, HashWrite, DelimPrinter};
use crate::cleaning::*;
use crate::db::{DbOpts, CopyRequest};
use crate::tracking::StageOpts;
use crate::logging::set_progress;
use super::Command;

/// Process OpenLib data into format suitable for PostgreSQL import.
#[derive(StructOpt)]
#[structopt(name="import-json")]
pub struct ImportJson {
  #[structopt(flatten)]
  db: DbOpts,

  #[structopt(flatten)]
  stage: StageOpts,

  /// Truncate the table before importing
  #[structopt(long="truncate")]
  truncate: bool,

  /// TOML spec file that describes the input
  #[structopt(name="SPEC")]
  spec: PathBuf,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

#[derive(Deserialize, Debug)]
enum ColOp {
  #[serde(rename="_")]
  Skip,
  #[serde(rename="str")]
  String,
  #[serde(rename="json")]
  JSON
}

/// Import specification read from TOML
#[derive(Deserialize, Debug)]
struct ImportSpec {
  schema: String,
  table: String,
  columns: Vec<String>,
  #[serde(default)]
  format: Vec<ColOp>
}

impl ImportSpec {
  fn import<R: BufRead, W: Write>(&self, src: &mut R, dst: &mut W) -> Result<usize> {
    if self.format.is_empty() {
      self.import_raw(src, dst)
    } else {
      self.import_delim(src, dst)
    }
  }

  fn import_raw<R: BufRead, W: Write>(&self, src: &mut R, dst: &mut W) -> Result<usize> {
    let mut jsbuf = String::new();
    let mut n = 0;
    for line in src.lines() {
      let json = line?;
      clean_json(&json, &mut jsbuf);
      write_pgencoded(dst, jsbuf.as_bytes())?;
      dst.write_all(b"\n")?;
      n += 1;
    }

    Ok(n)
  }

  fn import_delim<R: BufRead, W: Write>(&self, src: &mut R, dst: &mut W) -> Result<usize> {
    let mut jsbuf = String::new();
    let mut n = 0;
    for line in src.lines() {
      let line = line?;
      let mut delim = DelimPrinter::new("\t", "\n");
      let split = line.split("\t");
      for (fld, fc) in split.zip(&self.format) {
        match fc {
          ColOp::Skip => (),
          ColOp::String => {
            delim.preface(dst)?;
            write_pgencoded(dst, fld.as_bytes())?;
          },
          ColOp::JSON => {
            delim.preface(dst)?;
            clean_json(&fld, &mut jsbuf);
            write_pgencoded(dst, jsbuf.as_bytes())?;
          }
        }
      }
      delim.end(dst)?;
      n += 1;
    }
    info!("processed {} lines", n);
    Ok(n)
  }
}

impl Command for ImportJson {
  fn exec(self) -> Result<()> {
    info!("reading spec from {:?}", &self.spec);
    let spec = read_to_string(&self.spec)?;
    let spec: ImportSpec = toml::from_str(&spec)?;

    let dbo = self.db.default_schema(&spec.schema);

    let dbc = dbo.open()?;
    self.stage.begin_stage(&dbc)?;

    // Set up the input file, tracking read progress
    let infn = &self.infile;
    info!("reading from {:?}", infn);
    let fs = File::open(infn)?;
    let pb = ProgressBar::new(fs.metadata()?.len());
    pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})"));
    let _pbl = set_progress(&pb);

    // We want to hash the file while we read it
    let mut in_hash = Sha1::new();
    let read = HashRead::create(fs, &mut in_hash);
    // And wrap it in progress
    let pbr = pb.wrap_read(read);
    let pbr = BufReader::new(pbr);
    let gzf = MultiGzDecoder::new(pbr);
    let mut bfs = BufReader::new(gzf);

    // Set up the output stream, writing to the database
    let req = CopyRequest::new(&dbo, &spec.table)?;
    let req = req.with_schema(dbo.schema());
    let cref: Vec<&str> = spec.columns.iter().map(String::as_str).collect();
    let req = req.with_columns(&cref);
    let req = req.truncate(self.truncate);
    let out = req.open()?;
    let mut out_hash = Sha1::new();
    let hout = HashWrite::create(out, &mut out_hash);
    let mut buf_out = BufWriter::new(hout);

    // Actually run the import
    let n = spec.import(&mut bfs, &mut buf_out)?;
    buf_out.flush()?;
    drop(buf_out);

    // Grab the hashes and save them to the transcript
    let in_hash = in_hash.hexdigest();
    let out_hash = out_hash.hexdigest();
    let mut t_out = self.stage.open_transcript()?;
    info!("loaded {} records with hash {}", n, out_hash);
    writeln!(&mut t_out, "SOURCE {:?}", infn)?;
    writeln!(&mut t_out, "SHASH {}", in_hash)?;
    writeln!(&mut t_out, "HASH {}", out_hash)?;

    // All done! Record success and exit.
    self.stage.record_file(&dbc, infn, &in_hash)?;
    self.stage.end_stage(&dbc, &Some(out_hash))?;
    Ok(())
  }
}
