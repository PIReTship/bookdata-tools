use std::collections::HashMap;

use anyhow::{anyhow, Result};
use log::*;

use super::sources::*;
use super::{BookID, IdGraph, IdNode};
use polars::prelude::*;

type NodeMap = HashMap<i32, IdNode>;

struct GraphBuilder {
    graph: IdGraph,
    nodes: NodeMap,
}

impl GraphBuilder {
    fn add_vertices<R: NodeRead>(&mut self, src: R) -> Result<()> {
        info!("scanning vertices from {:?}", src);
        let node_df = src.read_node_ids()?;
        debug!("node schema: {:?}", node_df.schema());
        let mut node_df = node_df.collect()?;
        let ninit = self.nodes.len();

        // pull out the column to reduce memory
        let code_s = node_df.drop_in_place("code")?;
        let code_s = code_s.cast(&DataType::Int32)?;
        let codes = code_s.i32()?;
        let labels = node_df.column("label").ok().map(|c| c.utf8()).transpose()?;
        for i in 0..codes.len() {
            let code = codes.get(i).unwrap();
            let label = labels.map(|c| c.get(i)).flatten();
            let label = label.map(|s| s.to_string());
            let entry = self.nodes.entry(code);
            entry.or_insert_with(|| {
                self.graph.add_node(BookID {
                    code,
                    label,
                    cluster: 0,
                })
            });
        }

        info!(
            "loaded {} new vertices from {:?}",
            self.nodes.len() - ninit,
            src
        );

        Ok(())
    }

    fn add_edges<R: EdgeRead>(&mut self, src: R) -> Result<()> {
        info!("scanning edges from {:?}", src);
        let edge_df = src.read_edges()?;
        debug!("edge schema: {:?}", edge_df.schema());
        let edge_df = edge_df.collect()?;
        let src_s = edge_df.column("src")?.cast(&DataType::Int32)?;
        let srcs = src_s.i32()?;
        let dst_s = edge_df.column("dst")?.cast(&DataType::Int32)?;
        let dsts = dst_s.i32()?;

        let iter = srcs.into_iter().zip(dsts.into_iter());
        let mut n = 0;

        for pair in iter {
            if let (Some(sn), Some(dn)) = pair {
                let sid = self
                    .nodes
                    .get(&sn)
                    .ok_or_else(|| anyhow!("unknown source node {}", sn))?;
                let did = self
                    .nodes
                    .get(&dn)
                    .ok_or_else(|| anyhow!("unknown destination node {}", sn))?;
                self.graph.add_edge(*sid, *did, ());
                n += 1;
            }
        }

        info!("added {} edges from {:?}", n, src);

        Ok(())
    }
}

pub fn construct_graph() -> Result<IdGraph> {
    let graph = IdGraph::new_undirected();
    let nodes = NodeMap::new();
    let mut gb = GraphBuilder { graph, nodes };

    info!("loading nodes");
    gb.add_vertices(ISBN)?;
    gb.add_vertices(LOC)?;
    gb.add_vertices(OLEditions)?;
    gb.add_vertices(OLWorks)?;
    gb.add_vertices(GRBooks)?;
    gb.add_vertices(GRWorks)?;

    info!("loading edges");
    gb.add_edges(LOC)?;
    gb.add_edges(OLEditions)?;
    gb.add_edges(OLWorks)?;
    gb.add_edges(GRBooks)?;
    gb.add_edges(GRWorks)?;

    let graph = gb.graph;
    info!(
        "graph has {} nodes, {} edges",
        graph.node_count(),
        graph.edge_count()
    );
    Ok(graph)
}
