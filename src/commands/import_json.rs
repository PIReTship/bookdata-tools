use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::fs::File;
use std::path::PathBuf;

use log::*;

use structopt::StructOpt;
use flate2::bufread::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};

use crate::cleaning::{write_pgencoded, clean_json};
use crate::tsv::split_first;
use crate::db::{DbOpts, CopyRequest};
use crate::error::{Result, err};

#[derive(StructOpt, Debug)]
struct ImportInfo {
  /// Table name
  #[structopt(name = "TABLE")]
  table: String,
  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}


#[derive(Debug, StructOpt)]
enum ImportType {
  #[structopt(name="openlib")]
  OpenLib(ImportInfo),
  #[structopt(name="goodreads")]
  GoodReads(ImportInfo)
}

/// Process OpenLib data into format suitable for PostgreSQL import.
#[derive(StructOpt, Debug)]
#[structopt(name="import-json")]
pub struct Options {
  #[structopt(flatten)]
  db: DbOpts,

  /// Truncate the table before importing
  #[structopt(long="truncate")]
  truncate: bool,

  /// Specify the type of dataset to work on
  #[structopt(subcommand)]
  dataset: ImportType
}

fn process_openlib<R: BufRead, W: Write>(src: &mut R, dst: &mut W) -> Result<usize> {
  let mut jsbuf = String::new();
  let fail = || err("bad line");
  let mut n = 0;
  for line in src.lines() {
    let ls = line?;
    let (_ty, rest) = split_first(&ls).ok_or_else(fail)?;
    let (key, rest) = split_first(rest).ok_or_else(fail)?;
    let (_ver, rest) = split_first(rest).ok_or_else(fail)?;
    let (_stamp, json) = split_first(rest).ok_or_else(fail)?;
    clean_json(json, &mut jsbuf);
    dst.write_all(key.as_bytes())?;
    dst.write_all(b"\t")?;
    write_pgencoded(dst, jsbuf.as_bytes())?;
    dst.write_all(b"\n")?;
    n += 1
  }

  Ok(n)
}

fn process_raw<R: BufRead, W: Write>(src: &mut R, dst: &mut W) -> Result<usize> {
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

impl ImportType {
  fn table_name(&self) -> String {
    match self {
      ImportType::OpenLib(ref ii) => ii.table.clone(),
      ImportType::GoodReads(ref ii) => format!("raw_{}", ii.table)
    }
  }

  fn schema(&self) -> &'static str {
    match self {
      ImportType::OpenLib(_) => "ol",
      ImportType::GoodReads(_) => "gr",
    }
  }

  fn info<'a>(&'a self) -> &'a ImportInfo {
    match self {
      ImportType::OpenLib(ref ii) => ii,
      ImportType::GoodReads(ref ii) => ii
    }
  }

  fn columns(&self) -> Vec<String> {
    match self {
      ImportType::OpenLib(ref ii) => vec![format!("{}_key", ii.table), format!("{}_data", ii.table)],
      ImportType::GoodReads(ref ii) => vec![format!("gr_{}_data", ii.table)]
    }
  }

  fn import<R: BufRead, W: Write>(&self, src: &mut R, dst: &mut W) -> Result<usize> {
    match self {
      ImportType::OpenLib(_) => process_openlib(src, dst),
      ImportType::GoodReads(_) => process_raw(src, dst)
    }
  }
}

pub fn exec(opt: Options) -> Result<()> {
  let dbo = opt.db.default_schema(opt.dataset.schema());

  let infn = &opt.dataset.info().infile;
  info!("reading from {:?}", infn);
  let fs = File::open(infn)?;
  let pb = ProgressBar::new(fs.metadata()?.len());
  pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})"));

  let pbr = pb.wrap_read(fs);
  let pbr = BufReader::new(pbr);
  let gzf = MultiGzDecoder::new(pbr);
  let mut bfs = BufReader::new(gzf);

  let req = CopyRequest::new(&dbo, &opt.dataset.table_name())?;
  let req = req.with_schema(dbo.schema());
  let columns = opt.dataset.columns();
  let cref: Vec<&str> = columns.iter().map(String::as_str).collect();
  let req = req.with_columns(&cref);
  let req = req.truncate(opt.truncate);
  let out = req.open()?;
  let mut out = BufWriter::new(out);

  let n = opt.dataset.import(&mut bfs, &mut out)?;
  info!("loaded {} records", n);
  Ok(())
}
