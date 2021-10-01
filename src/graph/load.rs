use std::collections::{HashMap};

use log::*;
use anyhow::{Result, anyhow};
use futures::stream::StreamExt;

use datafusion::prelude::*;
use datafusion::physical_plan::execute_stream;

use crate::arrow::fusion::*;
use crate::arrow::row_de::RecordBatchDeserializer;
use super::sources::*;
use super::{BookID, IdGraph,IdNode};

type NodeMap = HashMap<i32, IdNode>;

async fn add_vertices(g: &mut IdGraph, nodes: &mut NodeMap, ctx: &mut ExecutionContext, src: &dyn NodeRead) -> Result<()> {
  info!("scanning vertices from {:?}", src);
  let node_df = src.read_node_ids(ctx)?;
  let plan = plan_df(ctx, node_df)?;
  let batches = execute_stream(plan).await?;
  let ninit = nodes.len();

  let mut iter = RecordBatchDeserializer::for_stream(batches);
  while let Some(row) = iter.next().await {
    let row: BookID = row?;
    if !nodes.contains_key(&row.code) {
      nodes.insert(row.code, g.add_node(row));
    }
  }

  info!("loaded {} new vertices from {:?}", nodes.len() - ninit, src);

  Ok(())
}

async fn add_edges(g: &mut IdGraph, nodes: &NodeMap, ctx: &mut ExecutionContext, src: &dyn EdgeRead) -> Result<()> {
  info!("scanning edges from {:?}", src);
  let edge_df = src.read_edges(ctx)?;
  let plan = plan_df(ctx, edge_df)?;
  let batches = execute_stream(plan).await?;
  let mut iter = RecordBatchDeserializer::for_stream(batches);
  let mut n = 0;

  while let Some(row) = iter.next().await {
    let row: (i32, i32) = row?;
    let (sn, dn) = row;

    let sid = nodes.get(&sn).ok_or_else(|| {
      anyhow!("unknown source node {}", sn)
    })?;
    let did = nodes.get(&dn).ok_or_else(|| {
      anyhow!("unknown destination node {}", sn)
    })?;
    g.add_edge(*sid, *did, ());
    n += 1;
  }

  info!("added {} edges from {:?}", n, src);

  Ok(())
}

pub async fn construct_graph() -> Result<IdGraph> {
  let mut graph = IdGraph::new_undirected();
  let mut nodes = NodeMap::new();
  let mut ctx = ExecutionContext::new();
  info!("loading nodes");
  for src in node_sources() {
    add_vertices(&mut graph, &mut nodes, &mut ctx, src.as_ref()).await?;
  }
  for src in edge_sources() {
    add_edges(&mut graph, &nodes, &mut ctx, src.as_ref()).await?;
  }
  info!("graph has {} nodes, {} edges", graph.node_count(), graph.edge_count());
  Ok(graph)
}
