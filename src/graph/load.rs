use std::collections::{HashMap, HashSet};

use log::*;
use anyhow::Result;
use futures::stream::StreamExt;

use datafusion::prelude::*;
use arrow::array::*;

use crate::arrow::fusion::*;
use super::sources::*;
use super::{IdGraph,IdNode};

type NodeMap = HashMap<i32, IdNode>;

#[allow(dead_code)]
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

  info!("loaded {} vertices from {:?}", node_ids.len(), src);

  for id in node_ids {
    g.add_node(id);
  }

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
  // info!("loading nodes");
  // for src in node_sources() {
  //   add_vertices(&mut graph, &mut ctx, src.as_ref()).await?;
  // }
  // info!("indexing nodes");
  let mut nodes = NodeMap::new();
  for src in edge_sources() {
    add_edges(&mut graph, &mut nodes, &mut ctx, src.as_ref()).await?;
  }
  info!("graph has {} nodes, {} edges", graph.node_count(), graph.edge_count());
  Ok(graph)
}
