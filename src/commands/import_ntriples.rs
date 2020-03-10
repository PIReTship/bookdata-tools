use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::collections::HashSet;
use std::thread;
use std::fs;
use std::path::{PathBuf};

use log::*;

use structopt::StructOpt;
use zip::read::ZipArchive;
use indicatif::{ProgressBar, ProgressStyle};
use uuid::Uuid;
use anyhow::{Result, anyhow};

use ntriple::parser::quiet_line;
use ntriple::{Subject, Predicate, Object};

use crossbeam_channel::{Sender, Receiver, bounded};

use crate::cleaning::{write_pgencoded};
use crate::db;
use crate::logging;
use super::Command;

const NODE_BATCH_SIZE: u64 = 20000;
const NODE_QUEUE_SIZE: usize = 2500;

/// Import n-triples RDF (e.g. from LOC) into a database.
#[derive(StructOpt, Debug)]
#[structopt(name="import-ntriples")]
pub struct ImportNtriples {
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
struct NodeSink {
  thread: Option<thread::JoinHandle<u64>>,
  send: Sender<NodeMsg>
}

/// Node sink worker code
struct NodePlumber {
  seen: HashSet<Uuid>,
  query: String,
  table: String,
  source: Receiver<NodeMsg>,
}

impl NodePlumber {
  fn create(src: Receiver<NodeMsg>, schema: &str) -> NodePlumber {
    NodePlumber {
      seen: HashSet::new(),
      query: format!("INSERT INTO {}.nodes (node_uuid, node_iri) VALUES ($1, $2) ON CONFLICT DO NOTHING", schema),
      table: format!("{}.nodes", schema),
      source: src
    }
  }

  /// Listen for messages and store in the database
  fn run(&mut self, url: &str) -> Result<u64> {
    let db = db::connect(url)?;
    let warm_tq = format!("SELECT pg_prewarm('{}', 'read')", self.table);
    let warm_iq = format!("SELECT pg_prewarm(indexrelid) FROM pg_index WHERE indrelid = '{}'::regclass", self.table);
    let ntb = db.execute(&warm_tq, &[])?;
    info!("prewarmed {} node table blocks", ntb);
    let nib = db.execute(&warm_iq, &[])?;
    info!("prewarmed {} node index blocks", nib);
    let mut cfg = postgres::transaction::Config::new();
    cfg.isolation_level(postgres::transaction::IsolationLevel::ReadUncommitted);
    let mut added = 0;
    let mut done = false;
    while !done {
    let txn = db.transaction_with(&cfg)?;
      txn.execute("SET LOCAL synchronous_commit TO OFF", &[])?;
      let (n, eos) = self.run_batch(&txn)?;
      debug!("committing {} new nodes", n);
      txn.commit()?;
      added += n;
      done = eos;
    }
    Ok(added)
  }

  /// Run a single batch of inserts
  fn run_batch(&mut self, db: &dyn postgres::GenericConnection) -> Result<(u64, bool)> {
    let stmt = db.prepare_cached(&self.query)?;
    let mut n = 0;
    while n < NODE_BATCH_SIZE {
      match self.source.recv()? {
        NodeMsg::SaveNode(id, iri) => {
          if self.seen.insert(id) {
            n += stmt.execute(&[&id, &iri])?;
          }
        },
        NodeMsg::Close => {
          return Ok((n, true));
        }
      }
    };
    Ok((n, false))
  }
}

impl NodeSink {
  fn create(db: &db::DbOpts) -> NodeSink {
    let opts = db.clone();
    let (tx, rx) = bounded(NODE_QUEUE_SIZE);

    let tb = thread::Builder::new().name("insert-nodes".to_string());
    let jh = tb.spawn(move || {
      let mut plumber = NodePlumber::create(rx, opts.schema());
      let url = opts.url().unwrap();
      plumber.run(&url).unwrap()
    }).unwrap();

    NodeSink {
      thread: Some(jh),
      send: tx
      }
    }

  fn save(&self, id: &Uuid, iri: &str) -> Result<()> {
    match self.send.send(NodeMsg::SaveNode(*id, iri.to_string())) {
      Ok(_) => Ok(()),
      Err(_) => Err(anyhow!("node channel disconnected"))
    }
  }
}

impl Drop for NodeSink {
  fn drop(&mut self) {
    self.send.send(NodeMsg::Close).unwrap();
    if let Some(thread) = self.thread.take() {
      match thread.join() {
        Ok(n) => info!("saved {} nodes", n),
        Err(e) => error!("node save thread failed: {:?}", e)
      };
    } else {
      error!("node sink already dropped");
    }
  }
}

struct IdGenerator {
  blank_ns: Uuid,
  node_sink: NodeSink
}

#[derive(Debug)]
enum Target<'a> {
  Node(Uuid),
  Literal(&'a ntriple::Literal)
}

impl IdGenerator {
  fn create(nodes: NodeSink, name: &str) -> IdGenerator {
    let ns_ns = Uuid::new_v5(&uuid::NAMESPACE_URL, "https://boisestate.github.io/bookdata/ns/blank");
    let blank_ns = Uuid::new_v5(&ns_ns, name);
    IdGenerator {
      blank_ns: blank_ns,
      node_sink: nodes
    }
  }

  fn node_id(&mut self, iri: &str) -> Result<Uuid> {
    let uuid = Uuid::new_v5(&uuid::NAMESPACE_URL, iri);
    self.node_sink.save(&uuid, iri)?;
    Ok(uuid)
  }

  fn blank_id(&self, key: &str) -> Result<Uuid> {
    let uuid = Uuid::new_v5(&self.blank_ns, key);
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

  fn obj_id<'a>(&mut self, obj: &'a Object) -> Result<Target<'a>> {
    match obj {
      Object::IriRef(iri) => self.node_id(iri).map(Target::Node),
      Object::BNode(key) => self.blank_id(key).map(Target::Node),
      Object::Lit(l) => Ok(Target::Literal(&l))
    }
  }
}

impl Command for ImportNtriples {
  fn exec(self) -> Result<()> {
    let inf = self.infile.as_path();
    let fs = fs::File::open(inf)?;
    let fs = BufReader::new(fs);
    let mut zf = ZipArchive::new(fs)?;
    if zf.len() > 1 {
      error!("{:?}: more than one member file", inf);
      return Err(anyhow!("too many input files"))
    } else if zf.len() == 0 {
      error!("{:?}: empty input archive", inf);
      return Err(anyhow!("empty input archive"));
    }
    let member = zf.by_index(0)?;
    info!("processing member {:?} with {} bytes", member.name(), member.size());

    let node_sink = NodeSink::create(&self.db);

    let lit_tbl = format!("{}_literals", self.prefix);
    let lit_cpy = db::CopyRequest::new(&self.db, &lit_tbl)?.with_name("literals");
    let lit_cpy = lit_cpy.with_schema(self.db.schema());
    let lit_cpy = lit_cpy.truncate(self.truncate);
    let lit_out = lit_cpy.open()?;
    let mut lit_out = BufWriter::new(lit_out);

    let triple_tbl = format!("{}_triples", self.prefix);
    let triple_cpy = db::CopyRequest::new(&self.db, &triple_tbl)?.with_name("triples");
    let triple_cpy = triple_cpy.with_schema(self.db.schema());
    let triple_cpy = triple_cpy.truncate(self.truncate);
    let triples_out = triple_cpy.open()?;
    let mut triples_out = BufWriter::new(triples_out);

    let mut idg = IdGenerator::create(node_sink, member.name());

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
          let o_id = idg.obj_id(&tr.object)?;
          match o_id {
            Target::Node(oid) => {
              write!(&mut triples_out, "{}\t{}\t{}\n", s_id, p_id, oid)?;
            },
            Target::Literal(l) => {
              write!(&mut lit_out, "{}\t{}\t", s_id, p_id)?;
              write_pgencoded(&mut lit_out, l.data.as_bytes())?;
              match l.data_type {
                ntriple::TypeLang::Lang(ref s) => {
                  write!(&mut lit_out, "\t")?;
                  write_pgencoded(&mut lit_out, s.as_bytes())?;
                  write!(&mut lit_out, "\n")?;
                },
                _ => {
                  write!(&mut lit_out, "\t\\N\n")?;
                }
              }
            }
          };
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
}
