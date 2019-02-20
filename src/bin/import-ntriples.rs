extern crate structopt;
#[macro_use]
extern crate log;
extern crate flate2;
extern crate indicatif;
extern crate bookdata;
extern crate zip;
extern crate postgres;
extern crate ntriple;
extern crate snap;
extern crate uuid;

use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::collections::HashSet;

use structopt::StructOpt;
use std::fs;
use std::path::{Path, PathBuf};
use zip::read::ZipArchive;
use indicatif::{ProgressBar, ProgressStyle};
use postgres::Connection;
use uuid::Uuid;

use ntriple::parser::triple_line;
use ntriple::{Subject, Predicate, Object};

use bookdata::cleaning::{write_pgencoded};
use bookdata::{log_init, Result};

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
  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf,
  /// Output directory
  #[structopt(name = "OUTPUT", parse(from_os_str))]
  outdir: PathBuf
}

struct IdGenerator<W: Write> {
  blank_ns: Uuid,
  node_file: W,
  lit_file: W,
  seen_uuids: HashSet<Uuid>
}

impl<W: Write> IdGenerator<W> {
  fn create(node_out: W, lit_out: W, name: &str) -> IdGenerator<W> {
    let ns_ns = Uuid::new_v5(&Uuid::NAMESPACE_URL, "https://boisestate.github.io/bookdata/ns/blank".as_bytes());
    let blank_ns = Uuid::new_v5(&ns_ns, name.as_bytes());
    IdGenerator {
      blank_ns: blank_ns,
      node_file: node_out,
      lit_file: lit_out,
      seen_uuids: HashSet::new()
    }
  }

  fn node_id(&mut self, iri: &str) -> Result<Uuid> {
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, iri.as_bytes());
    if !self.seen_uuids.contains(&uuid) {
      write!(&mut self.node_file, "{}\t{}\n", uuid, iri)?;
    } else {
      self.seen_uuids.insert(uuid);
    }
    Ok(uuid)
  }

  fn blank_id(&self, key: &str) -> Result<Uuid> {
    let uuid = Uuid::new_v5(&self.blank_ns, key.as_bytes());
    Ok(uuid)
  }

  fn lit_id(&mut self, lit: &str) -> Result<Uuid> {
    let uuid = Uuid::new_v4();
    write!(&mut self.lit_file, "{}\t", uuid)?;
    write_pgencoded(&mut self.lit_file, lit.as_bytes())?;
    self.lit_file.write_all(b"\n")?;
    Ok(uuid)
  }
  
  fn subj_id(&mut self, sub: &Subject) -> Result<Uuid> {
    match sub {
      Subject::IriRef(iri) => self.node_id(iri),
      Subject::BNode(key) => self.blank_id(key)
    }
  }

  fn pred_id(&mut self, pred: &Predicate) -> Result<Uuid> {
    match pred {
      Predicate::IriRef(iri) => self.node_id(iri)
    }
  }

  fn obj_id(&mut self, obj: &Object) -> Result<Uuid> {
    match obj {
      Object::IriRef(iri) => self.node_id(iri),
      Object::BNode(key) => self.blank_id(key),
      Object::Lit(l) => self.lit_id(&l.data)
    }
  }
}

fn open_out(dir: &Path, name: &str) -> Result<Box<Write>> {
  let mut buf = dir.to_path_buf();
  buf.push(name);
  let file = fs::OpenOptions::new().write(true).create(true).open(buf)?;
  let file = snap::Writer::new(file);
  let file = BufWriter::new(file);
  Ok(Box::new(file))
}

fn main() -> Result<()> {
  let opt = Opt::from_args();
  log_init(opt.quiet, opt.verbose)?;

  let inf = opt.infile.as_path();
  let fs = fs::File::open(inf)?;
  let fs = BufReader::new(fs);
  let mut zf = ZipArchive::new(fs)?;
  if zf.len() > 1 {
    error!("{:?}: more than one member file", inf);
    return Err(bookdata::err("too many input files"))
  } else if zf.len() == 0 {
    error!("{:?}: empty input archive", inf);
    return Err(bookdata::err("empty input archive"));
  }
  let member = zf.by_index(0)?;
  info!("processing member {:?} with {} bytes", member.name(), member.size());
  
  let outp = opt.outdir.as_path();
  if !outp.is_dir() {
    fs::create_dir_all(&outp)?;
  }

  let node_out = open_out(&outp, "nodes.snappy")?;
  let lit_out = open_out(&outp, "literals.snappy")?;
  let mut triples_out = open_out(&outp, "triples.snappy")?;

  let mut idg = IdGenerator::create(node_out, lit_out, member.name());

  let pb = ProgressBar::new(member.size());
  pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})"));
  let pbr = pb.wrap_read(member);
  let pbr = BufReader::new(pbr);
  let mut lno = 0;
  for line in pbr.lines() {
    let line = line?;
    lno += 1;
    match triple_line(&line) {
      Ok(Some(tr)) => {
        let s_id = idg.subj_id(&tr.subject)?;
        let p_id = idg.pred_id(&tr.predicate)?;
        let o_id = idg.obj_id(&tr.object)?;
        write!(&mut triples_out, "{}\t{}\t{}\n", s_id, p_id, o_id)?
      },
      Ok(None) => (),
      Err(ref e) if pb.is_hidden() => {
        error!("error on line {}: {:?}", lno, e);
        error!("invalid line contained: {}", line);
      },
      Err(ref e) => {
        pb.println(format!("error on line {}: {:?}", lno, e));
      }
    };
  }

  Ok(())
}
