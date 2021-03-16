use std::collections::HashMap;

use log::*;
use anyhow::Result;
use fallible_iterator::FallibleIterator;
use postgres::Connection;

use super::sources::*;
use super::{IdGraph,IdNode};

type NodeMap = HashMap<i32, IdNode>;

fn add_vertices<Q: QueryContext>(g: &mut IdGraph, db: &Connection, src: &dyn NodeQuery<Q>, ctx: &Q) -> Result<()> {
  info!("scanning vertices from {:?}", src);
  let query = src.q_node_ids(ctx);
  let txn = db.transaction()?;
  let stmt = txn.prepare(&query)?;
  let mut rows = stmt.lazy_query(&txn, &[], 1000)?;
  while let Some(row) = rows.next()? {
    let id: i32 = row.get(0);
    g.add_node(id);
  }
  Ok(())
}

fn add_edges<Q: QueryContext>(g: &mut IdGraph, nodes: &NodeMap, db: &Connection, src: &dyn EdgeQuery<Q>, ctx: &Q) -> Result<()> {
  info!("scanning edges from {:?}", src);
  let query = src.q_edges(ctx);
  let txn = db.transaction()?;
  let stmt = txn.prepare(&query)?;
  let mut rows = stmt.lazy_query(&txn, &[], 1000)?;
  while let Some(row) = rows.next()? {
    let src: i32 = row.get(0);
    let dst: i32 = row.get(1);
    let sn = nodes.get(&src).unwrap();
    let dn = nodes.get(&dst).unwrap();
    g.add_edge(*sn, *dn, ());
  }
  Ok(())
}

fn vert_map(g: &IdGraph) -> NodeMap {
  let mut map = NodeMap::with_capacity(g.node_count());
  for ni in g.node_indices() {
    let vref = g.node_weight(ni).unwrap();
    map.insert(*vref, ni);
  }
  info!("indexed {} nodes", map.len());
  map
}

pub fn load_graph(db: &Connection) -> Result<IdGraph> {
  let mut graph = IdGraph::new_undirected();
  let ctx = FullTable;
  for src in node_sources() {
    add_vertices(&mut graph, db, src.as_ref(), &ctx)?;
  }
  let nodes = vert_map(&graph);
  for src in edge_sources() {
    add_edges(&mut graph, &nodes, &db, src.as_ref(), &ctx)?;
  }
  Ok(graph)
}
