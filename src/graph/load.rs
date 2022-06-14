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

struct GraphBuilder {
  graph: IdGraph,
  nodes: NodeMap,
  ctx: SessionContext
}

impl GraphBuilder {
  async fn add_vertices<R: NodeRead>(&mut self, src: R) -> Result<()> {
    info!("scanning vertices from {:?}", src);
    let node_df = src.read_node_ids(&mut self.ctx).await?;
    let plan = plan_df(&mut self.ctx, node_df).await?;
    let batches = execute_stream(plan, self.ctx.task_ctx()).await?;
    let ninit = self.nodes.len();

    let mut iter = RecordBatchDeserializer::for_stream(batches);
    while let Some(row) = iter.next().await {
      let row: BookID = row?;
      let entry = self.nodes.entry(row.code);
      entry.or_insert_with(|| self.graph.add_node(row));
    }

    info!("loaded {} new vertices from {:?}", self.nodes.len() - ninit, src);

    Ok(())
  }

  async fn add_edges<R: EdgeRead>(&mut self, src: R) -> Result<()> {
    info!("scanning edges from {:?}", src);
    let edge_df = src.read_edges(&mut self.ctx).await?;
    let plan = plan_df(&mut self.ctx, edge_df).await?;
    let batches = execute_stream(plan, self.ctx.task_ctx()).await?;
    let mut iter = RecordBatchDeserializer::for_stream(batches);
    let mut n = 0;

    while let Some(row) = iter.next().await {
      let row: (i32, i32) = row?;
      let (sn, dn) = row;

      let sid = self.nodes.get(&sn).ok_or_else(|| {
        anyhow!("unknown source node {}", sn)
      })?;
      let did = self.nodes.get(&dn).ok_or_else(|| {
        anyhow!("unknown destination node {}", sn)
      })?;
      self.graph.add_edge(*sid, *did, ());
      n += 1;
    }

    info!("added {} edges from {:?}", n, src);

    Ok(())
  }
}

pub async fn construct_graph() -> Result<IdGraph> {
  let graph = IdGraph::new_undirected();
  let nodes = NodeMap::new();
  let ctx = SessionContext::new();
  let mut gb = GraphBuilder {
    graph, nodes, ctx
  };

  info!("loading nodes");
  gb.add_vertices(ISBN).await?;
  gb.add_vertices(LOC).await?;
  gb.add_vertices(OLEditions).await?;
  gb.add_vertices(OLWorks).await?;
  gb.add_vertices(GRBooks).await?;
  gb.add_vertices(GRWorks).await?;

  info!("loading edges");
  gb.add_edges(LOC).await?;
  gb.add_edges(OLEditions).await?;
  gb.add_edges(OLWorks).await?;
  gb.add_edges(GRBooks).await?;
  gb.add_edges(GRWorks).await?;

  let graph = gb.graph;
  info!("graph has {} nodes, {} edges", graph.node_count(), graph.edge_count());
  Ok(graph)
}
