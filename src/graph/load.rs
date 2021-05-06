use std::collections::{HashMap, HashSet};

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

async fn add_vertices(g: &mut IdGraph, ctx: &mut ExecutionContext, src: &dyn NodeRead) -> Result<()> {
  info!("scanning vertices from {:?}", src);
  let node_df = src.read_node_ids(ctx)?;
  let batches = node_df.collect().await?;

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

pub async fn load_graph() -> Result<IdGraph> {
  let mut graph = IdGraph::new_undirected();
  let mut ctx = ExecutionContext::new();
  info!("loading nodes");
  for src in node_sources() {
    add_vertices(&mut graph, &mut ctx, src.as_ref()).await?;
  }
  info!("indexing nodes");
  let nodes = vert_map(&graph);
  // for src in edge_sources() {
  //   add_edges(&mut graph, &nodes, &db, src.as_ref(), &ctx)?;
  // }
  Ok(graph)
}
