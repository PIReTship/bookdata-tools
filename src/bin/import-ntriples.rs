#[macro_use]
extern crate structopt;
#[macro_use]
extern crate log;
extern crate flate2;
extern crate indicatif;
extern crate bookdata;
extern crate zip;
extern crate postgres;

use std::io::prelude::*;
use std::io::{self, BufReader};
use std::error::Error;
use std::collections::HashMap;

use structopt::StructOpt;
use std::fs::File;
use std::path::PathBuf;
use zip::read::ZipArchive;
use indicatif::{ProgressBar, ProgressStyle};
use postgres::Connection;

use bookdata::cleaning::{write_pgencoded, clean_json};
use bookdata::tsv::split_first;
use bookdata::log_init;

/// Import n-triples RDF (e.g. from LOC) into a database.
#[derive(StructOpt, Debug)]
#[structopt(name="import-ntriples")]
struct Opt {
  /// Verbose mode (-v, -vv, -vvv, etc.)
  #[structopt(short="v", long="verbose", parse(from_occurrences))]
  verbose: usize,
  /// Silence output
  #[structopt(short="q", long="quiet")]
  quiet: bool,
  /// Database URL to connect to
  #[structopt(long="db-url")]
  db_url: Option<String>,
  /// Database schema
  #[structopt(long="db-schema")]
  db_schema: Option<String>,
  /// Triple table
  #[structopt(long="table")]
  table: Option<String>,
  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

struct NodeIndex {
  table: HashMap<String,i64>,
  max: i64
}

impl NodeIndex {
  fn load(db: &Connection, opt: &Opt) -> Result<NodeIndex, Box<Error>> {
    let tbl = match &(opt.db_schema) {
      Some(s) => format!("{}.nodes", s),
      None => "nodes".to_string()
    };
    let query = format!("SELECT node_id, node_iri FROM {}", tbl);
    let mut table = HashMap::new();
    let mut last: i64 = 0;
    
    for row in &db.query(&query, &[])? {
      let id: i64 = row.get(0);
      let iri: String = row.get(1);
      table.insert(iri, id);
      if (id > last) {
        last = id;
      }
    }
    Ok(NodeIndex {
      table: table,
      max: last
    })
  }

  fn node_id(&mut self, iri: String) -> i64 {
    let id = self.table.entry(iri).or_insert(self.max + 1);
    let id = *id;
    if id > self.max {
      self.max = id;
    }
    id
  }
}

fn main() -> Result<(), Box<Error>> {
  let opt = Opt::from_args();
  log_init(opt.quiet, opt.verbose)?;

  let db = bookdata::db::db_open(&opt.db_url)?;
  let nodes = NodeIndex::load(&db, &opt)?;
  info!("database has {} nodes", nodes.table.len());

  let inf = opt.infile.as_path();
  let fs = File::open(inf)?;
  let mut zf = ZipArchive::new(fs)?;
  if zf.len() > 1 {
    error!("{:?}: more than one member file", inf);
    return Err(Box::<Error>::from("too many input files"));
  } else if zf.len() == 0 {
    error!("{:?}: empty input archive", inf);
    return Err(Box::<Error>::from("empty input archive"));
  }
  let member = zf.by_index(0)?;
  info!("processing member {:?} with {} bytes", member.name(), member.size());

  Ok(())
}
