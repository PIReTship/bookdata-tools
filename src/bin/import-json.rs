#[macro_use] extern crate log;
extern crate structopt;
extern crate flate2;
extern crate indicatif;
extern crate bookdata;

use std::io::prelude::*;
use std::io::{BufReader, BufWriter};

use structopt::StructOpt;
use std::fs::File;
use std::path::PathBuf;
use flate2::bufread::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};

use bookdata::cleaning::{write_pgencoded, clean_json};
use bookdata::tsv::split_first;
use bookdata::db::{DbOpts, truncate_table, copy_target};
use bookdata::{Result, LogOpts, err};

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
struct Opt {
  #[structopt(flatten)]
  logging: LogOpts,

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
      ImportType::OpenLib(_) => "openlib",
      ImportType::GoodReads(_) => "goodreads",
    }
  }

  fn info<'a>(&'a self) -> &'a ImportInfo {
    match self {
      ImportType::OpenLib(ref ii) => ii,
      ImportType::GoodReads(ref ii) => ii
    }
  }

  fn query(&self, schema: &str) -> String {
    match self {
      ImportType::OpenLib(ref ii) =>
        format!("COPY {schema}.{table} ({table}_key, {table}_data) FROM STDIN", schema=schema, table=ii.table),
      ImportType::GoodReads(ref ii) =>
        format!("COPY {schema}.raw_{table} (gr_{table}_data) FROM STDIN", schema=schema, table=ii.table)
    }
  }

  fn import<R: BufRead, W: Write>(&self, src: &mut R, dst: &mut W) -> Result<usize> {
    match self {
      ImportType::OpenLib(_) => process_openlib(src, dst),
      ImportType::GoodReads(_) => process_raw(src, dst)
    }
  }
}

fn main() -> Result<()> {
  let opt = Opt::from_args();
  opt.logging.init()?;
  let dbo = opt.db.default_schema(opt.dataset.schema());

  if opt.truncate {
    truncate_table(&dbo, &opt.dataset.table_name(), dbo.schema())?;
  }

  let infn = &opt.dataset.info().infile;
  info!("reading from {:?}", infn);
  let fs = File::open(infn)?;
  let pb = ProgressBar::new(fs.metadata()?.len());
  pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})"));

  let pbr = pb.wrap_read(fs);
  let pbr = BufReader::new(pbr);
  let gzf = MultiGzDecoder::new(pbr);
  let mut bfs = BufReader::new(gzf);

  let out = copy_target(&dbo, &opt.dataset.query(dbo.schema()), "copy")?;
  let mut out = BufWriter::new(out);

  let n = opt.dataset.import(&mut bfs, &mut out)?;
  info!("loaded {} records", n);
  Ok(())
}
