/// This module implements the _node sink_, which saves nodes in the RDF graph
/// to the database.
///
/// The node sink uses a worker thread to do the database work, so the main
/// thread can keep processing node text.  This module encpasulates all of
/// that through the exposed `NodeSink` interface.

use std::collections::HashSet;
use std::thread;

use anyhow::{Result, anyhow};
use crossbeam_channel::{Sender, Receiver, bounded};
use uuid::Uuid;

use log::*;

use crate::db;

const NODE_BATCH_SIZE: u64 = 20000;
const NODE_QUEUE_SIZE: usize = 2500;

/// Sink for saving nodes
pub struct NodeSink {
  thread: Option<thread::JoinHandle<u64>>,
  send: Sender<NodeMsg>
}

/// Node sink worker interface
struct NodePlumber {
  seen: HashSet<Uuid>,
  query: String,
  table: String,
  source: Receiver<NodeMsg>,
}

/// Message for saving nodes in the other thread
#[derive(Debug)]
enum NodeMsg {
  SaveNode(Uuid, String),
  Close
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

  /// Listen for messages and store in the database.  This will be run in the worker thread.
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
  /// Create a new node sink to connect to the specified database.
  pub fn create(db: &db::DbOpts) -> NodeSink {
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

  /// Queue a node to be saved in the database.
  pub fn save(&self, id: &Uuid, iri: &str) -> Result<()> {
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
