use std::collections::{HashMap};

use log::*;
use anyhow::Result;
use futures::stream::StreamExt;

use datafusion::prelude::*;
use arrow::array::*;

use crate::arrow::fusion::*;
use super::sources::*;
use super::{IdGraph,IdNode};

type NodeMap = HashMap<i32, IdNode>;

async fn add_vertices(g: &mut IdGraph, nodes: &mut NodeMap, ctx: &mut ExecutionContext, src: &dyn NodeRead) -> Result<()> {
  info!("scanning vertices from {:?}", src);
  let node_df = src.read_node_ids(ctx)?;
  let batches = node_df.collect().await?;
  let ninit = nodes.len();

  for batch in batches {
    let col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    for id in col.iter().flatten() {
      if !nodes.contains_key(&id) {
        nodes.insert(id, g.add_node(id));
      }
    }
  }

  info!("loaded {} new vertices from {:?}", nodes.len() - ninit, src);

  Ok(())
}

async fn add_edges(g: &mut IdGraph, nodes: &mut NodeMap, ctx: &mut ExecutionContext, src: &dyn EdgeRead) -> Result<()> {
  info!("scanning edges from {:?}", src);
  let init_n = nodes.len();
  let edge_df = src.read_edges(ctx)?;
  let plan = plan_df(ctx, edge_df)?;
  let mut batches = run_plan(&plan).await?;

  while let Some(batch) = batches.next().await {
    let batch = batch?;
    let src = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let dst = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
    let iter = src.iter().zip(dst.iter());
    let iter = iter.map(|po| po.0.zip(po.1));
    for (sn, dn) in iter.flatten() {
      let sid = nodes.entry(sn).or_insert_with_key(|n| g.add_node(*n)).to_owned();
      let did = nodes.entry(dn).or_insert_with_key(|n| g.add_node(*n)).to_owned();
      g.add_edge(sid, did, ());
    }
  }

  info!("discovered {} new nodes from {:?}", nodes.len() - init_n, src);

  Ok(())
}

pub async fn load_graph() -> Result<IdGraph> {
  let mut graph = IdGraph::new_undirected();
  let mut ctx = ExecutionContext::new();
  let mut nodes = NodeMap::new();
  info!("loading nodes");
  for src in node_sources() {
    add_vertices(&mut graph, &mut nodes, &mut ctx, src.as_ref()).await?;
  }
  for src in edge_sources() {
    add_edges(&mut graph, &mut nodes, &mut ctx, src.as_ref()).await?;
  }
  info!("graph has {} nodes, {} edges", graph.node_count(), graph.edge_count());
  Ok(graph)
}
