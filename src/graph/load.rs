use std::collections::{HashMap, HashSet};

use log::*;
use anyhow::Result;

use datafusion::prelude::*;
use arrow::array::*;

use super::sources::*;
use super::{IdGraph,IdNode};

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

async fn add_edges(g: &mut IdGraph, nodes: &NodeMap, ctx: &mut ExecutionContext, src: &dyn EdgeRead) -> Result<()> {
  info!("scanning edges from {:?}", src);
  let edge_df = src.read_edges(ctx)?;
  let batches = edge_df.collect().await?;

  for batch in batches {
    let src = batch.column(0);
    let src = src.as_any().downcast_ref::<Int32Array>().expect("invalid column type");
    let dst = batch.column(1);
    let dst = dst.as_any().downcast_ref::<Int32Array>().expect("invalid column type");
    for (sn, dn) in src.iter().zip(dst.iter()) {
      match sn.zip(dn) {
        Some((sn, dn)) => {
          let sn = nodes.get(&sn).unwrap();
          let dn = nodes.get(&dn).unwrap();
          g.add_edge(*sn, *dn, ());
        },
        _ => ()
      }
    }
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
  for src in edge_sources() {
    add_edges(&mut graph, &nodes, &mut ctx, src.as_ref()).await?;
  }
  info!("graph has {} edges", graph.edge_count());
  Ok(graph)
}
