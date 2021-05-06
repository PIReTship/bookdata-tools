use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use log::*;
use anyhow::Result;
use fallible_iterator::FallibleIterator;
use postgres::Connection;

use datafusion::prelude::*;
use arrow::datatypes::*;
use arrow::array::*;

use super::sources::*;
use super::{IdGraph,IdNode};
use crate::arrow::fusion::*;

type NodeMap = HashMap<i32, IdNode>;

async fn init_graph(ctx: &mut ExecutionContext, nodes: Arc<dyn DataFrame>) -> Result<IdGraph> {
  let batches = nodes.collect().await?;
  let mut g = IdGraph::new_undirected();

  let mut node_ids = HashSet::new();

  for batch in batches {
    let col = batch.column(0);
    let col = col.as_any().downcast_ref::<Int32Array>().expect("invalid column type");
    for id in col.iter() {
      if let Some(id) = id {
        node_ids.insert(id);
      }
    }
  }

  for id in node_ids {
    g.add_node(id);
  }

  Ok(g)
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

pub async fn load_graph() -> Result<IdGraph> {
  let mut ctx = ExecutionContext::new();
  info!("loading nodes");
  let nodes = node_sources().iter().map(|s| s.read_node_ids(&mut ctx)).reduce(|d1, d2| {
    let d1 = d1?;
    let d2 = d2?;
    let du = d1.union(d2)?;
    Ok(du)
  }).unwrap()?;
  let mut graph = init_graph(&mut ctx, nodes).await?;
  info!("indexing nodes");
  let nodes = vert_map(&graph);
  // for src in edge_sources() {
  //   add_edges(&mut graph, &nodes, &db, src.as_ref(), &ctx)?;
  // }
  Ok(graph)
}
