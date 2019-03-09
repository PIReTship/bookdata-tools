extern crate structopt;
#[macro_use]
extern crate log;
extern crate flate2;
extern crate indicatif;
extern crate bookdata;
extern crate zip;
extern crate postgres;
extern crate ntriple;
extern crate uuid;
extern crate crossbeam_channel;

use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::collections::HashMap;
use std::thread;

use structopt::StructOpt;
use std::fs;
use std::path::{PathBuf};
use zip::read::ZipArchive;
use indicatif::{ProgressBar, ProgressStyle};
use uuid::Uuid;

use ntriple::parser::quiet_line;
use ntriple::{Subject, Predicate, Object};

use crossbeam_channel::{Sender, Receiver, bounded};

use bookdata::cleaning::{write_pgencoded};
use bookdata::{LogOpts, Result};
use bookdata::db;
use bookdata::logging;

const NODE_BATCH_SIZE: i32 = 20000;

/// Import n-triples RDF (e.g. from LOC) into a database.
#[derive(StructOpt, Debug)]
#[structopt(name="import-ntriples")]
struct Opt {
  #[structopt(flatten)]
  logging: LogOpts,

  #[structopt(flatten)]
  db: db::DbOpts,

  /// Database table prefix for data
  #[structopt(short="p", long="prefix")]
  prefix: String,
  /// Truncate the database tables
  #[structopt(long="truncate")]
  truncate: bool,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

/// Message for saving nodes in the other thread
#[derive(Debug)]
enum NodeMsg {
  SaveNode(Uuid, String),
  Close
}

/// Sink for saving nodes
struct NodeTable<'a> {
  db: &'a postgres::Connection,
  tx: Option<postgres::transaction::Transaction<'a>>,
  insert: postgres::stmt::Statement<'a>,
  lookup: postgres::stmt::Statement<'a>,
  tx_cfg: postgres::transaction::Config,
  inserted: i32,
  seen: HashMap<Uuid,i32>
}

enum Target<'a> {
  Triple(i32),
  Literal(&'a str)
}

impl <'a> NodeTable<'a> {
  fn create(opts: &db::DbOpts, db: &'a postgres::Connection) -> Result<NodeTable<'a>> {
    let table = format!("{}.nodes", opts.schema());

    let warm_tq = format!("SELECT pg_prewarm('{}', 'read')", table);
    let warm_iq = format!("SELECT pg_prewarm(indexrelid) FROM pg_index WHERE indrelid = '{}'::regclass", table);
    let ntb = db.execute(&warm_tq, &[])?;
    info!("prewarmed {} node table blocks", ntb);
    let nib = db.execute(&warm_iq, &[])?;
    info!("prewarmed {} node index blocks", nib);

    let insert = format!("INSERT INTO {}.nodes (node_uuid, node_iri) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING node_id", opts.schema());
    let lookup = format!("SELECT node_id FROM {}.nodes WHERE node_uuid = $1", opts.schema());

    let mut cfg = postgres::transaction::Config::new();
    cfg.isolation_level(postgres::transaction::IsolationLevel::ReadUncommitted);
    let txn = db.transaction_with(&cfg)?;

    Ok(NodeTable {
      db: db,
      tx: Some(txn),
      insert: db.prepare_cached(&insert)?,
      lookup: db.prepare_cached(&lookup)?,
      tx_cfg: cfg,
      inserted: 0,
      seen: HashMap::new()
    })
  }

  fn intern(&mut self, uuid: &Uuid, iri: &str) -> Result<i32> {
    let id = self.seen.get(&uuid);
    match id {
      Some(id) => Ok(*id),
      None => {
        let mut res = self.insert.query(&[&uuid, &iri])?;
        if res.is_empty() {
          // oops we must search
          res = self.lookup.query(&[&uuid])?;
        } else {
          self.inserted += 1;
          if self.inserted % NODE_BATCH_SIZE == 0 {
            if let Some(tx) = self.tx.take() {
              debug!("commiting nodes after {} inserts", self.inserted);
              tx.commit()?;
              let tx = self.db.transaction_with(&self.tx_cfg)?;
              self.tx = Some(tx);
            } else {
              panic!("transaction state mismatch");
            }
          }
        }
        assert!(res.len() == 1);
        let id: i32 = res.get(0).get(0);
        self.seen.insert(*uuid, id);
        Ok(id)
      }
    }
  }
}

impl <'a> Drop for NodeTable<'a> {
  fn drop(&mut self) {
    if let Some(tx) = self.tx.take() {
      tx.commit();
    }
    info!("saved {} nodes", self.inserted);
  }
}

struct IdGenerator<'db> {
  blank_ns: Uuid,
  node_table: &'db mut NodeTable<'db>,
  blank_ids: HashMap<Uuid,i32>,
  blank_last: i32
}

impl<'db> IdGenerator<'db> {
  fn create(nodes: &'db mut NodeTable<'db>, name: &str) -> IdGenerator<'db> {
    let ns_ns = Uuid::new_v5(&uuid::NAMESPACE_URL, "https://boisestate.github.io/bookdata/ns/blank");
    let blank_ns = Uuid::new_v5(&ns_ns, name);
    IdGenerator {
      blank_ns: blank_ns,
      node_table: nodes,
      blank_ids: HashMap::new(),
      blank_last: 0
    }
  }

  fn node_id(&mut self, iri: &str) -> Result<i32> {
    let uuid = Uuid::new_v5(&uuid::NAMESPACE_URL, iri);
    let id = self.node_table.intern(&uuid, iri)?;
    Ok(id)
  }

  fn blank_id(&mut self, key: &str) -> Result<i32> {
    let uuid = Uuid::new_v5(&self.blank_ns, key);
    let id = self.blank_ids.entry(uuid).or_insert_with(|| {
      let n = self.blank_last - 1;
      self.blank_last = n;
      n
    });
    Ok(*id)
  }

  fn subj_id(&mut self, sub: &Subject) -> Result<i32> {
    match sub {
      Subject::IriRef(iri) => self.node_id(iri),
      Subject::BNode(key) => self.blank_id(key)
    }
  }

  fn pred_id(&mut self, pred: &Predicate) -> Result<i32> {
    match pred {
      Predicate::IriRef(iri) => self.node_id(iri)
    }
  }

  fn obj_id<'a>(&mut self, obj: &'a Object) -> Result<Target<'a>> {
    match obj {
      Object::IriRef(iri) => self.node_id(iri).map(Target::Triple),
      Object::BNode(key) => self.blank_id(key).map(Target::Triple),
      Object::Lit(l) => Ok(Target::Literal(&l.data))
    }
  }
}

fn main() -> Result<()> {
  let opt = Opt::from_args();
  opt.logging.init()?;

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

  let db = opt.db.open()?;
  let node_table = NodeTable::create(&opt.db, &db)?;

  let lit_tbl = format!("{}_literal", opt.prefix);
  let lit_cpy = db::CopyRequest::new(&opt.db, &lit_tbl)?.with_name("literals");
  let lit_cpy = lit_cpy.with_schema(opt.db.schema());
  let lit_out = lit_cpy.open()?;
  let mut lit_out = BufWriter::new(lit_out);

  let triple_tbl = format!("{}_triple", opt.prefix);
  let triple_cpy = db::CopyRequest::new(&opt.db, &triple_tbl)?.with_name("triples");
  let triple_cpy = triple_cpy.with_schema(opt.db.schema());
  let triple_cpy = triple_cpy.truncate(opt.truncate);
  let triples_out = triple_cpy.open()?;
  let mut triples_out = BufWriter::new(triples_out);

  let mut idg = IdGenerator::create(&mut node_table, member.name());

  let pb = ProgressBar::new(member.size());
  pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})"));
  let pbr = pb.wrap_read(member);
  let pbr = BufReader::new(pbr);
  logging::set_progress(&pb);
  let mut lno = 0;
  for line in pbr.lines() {
    let line = line?;
    lno += 1;
    match quiet_line(&line) {
      Ok(Some(tr)) => {
        let s_id = idg.subj_id(&tr.subject)?;
        let p_id = idg.pred_id(&tr.predicate)?;
        match idg.obj_id(&tr.object)? {
          Target::Triple(o_id) => {
            write!(&mut triples_out, "{}\t{}\t{}\n", s_id, p_id, o_id)?;
          },
          Target::Literal(ref lv) => {
            write!(&mut lit_out, "{}\t{}\t", s_id, p_id)?;
            write_pgencoded(&mut lit_out, lv.as_bytes())?;
            write!(&mut lit_out, "\n")?;
          }
        }
      },
      Ok(None) => (),
      Err(ref e) => {
        error!("error on line {}: {:?}", lno, e);
        error!("invalid line contained: {}", line);
      }
    };
  }
  pb.finish();
  logging::clear_progress();

  Ok(())
}
