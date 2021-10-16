use std::path::Path;
use std::fs::File;
use std::collections::{HashSet, HashMap};
use zstd::{Encoder, Decoder};

use serde::{Serialize, Deserialize};
use petgraph::{Graph, Undirected};
use petgraph::graph::DefaultIx;
use petgraph::graph::NodeIndex;

use anyhow::Result;

/// A book identifier with optional label used as a graph node.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BookID {
  pub code: i32,
  pub label: Option<String>
}

pub type IdGraph = Graph<BookID, (), Undirected>;
pub type IdNode = NodeIndex<DefaultIx>;

mod sources;
mod load;
mod gml;

pub use load::construct_graph;
pub use gml::save_gml;

/// Save a graph to a compressed, encoded file.
pub fn save_graph<P: AsRef<Path>>(graph: &IdGraph, path: P) -> Result<()> {
  let file = File::create(path)?;
  let mut out = Encoder::new(file, 6)?;
  // out.multithread(2)?;
  rmp_serde::encode::write(&mut out, &graph)?;
  out.finish()?;
  Ok(())
}

/// Load a graph from a compressed, encoded file.
pub fn load_graph<P: AsRef<Path>>(path: P) -> Result<IdGraph> {
  let file = File::open(path)?;
  let rdr = Decoder::new(file)?;
  let g = rmp_serde::decode::from_read(rdr)?;
  Ok(g)
}

/// Filter a graph to contain only some nodes.
pub fn filter_to_nodes(graph: &IdGraph, nodes: &Vec<IdNode>) -> IdGraph {
  let mut g2 = IdGraph::with_capacity(nodes.len(), nodes.len());
  let mut emap = HashMap::new();

  for node in nodes {
    let w = graph.node_weight(*node).unwrap();
    let n2 = g2.add_node(w.clone());
    emap.insert(node, n2);
  }

  for src in nodes {
    for dst in graph.neighbors(*src) {
      if dst.index() > src.index() {
        let s2 = emap.get(src).unwrap();
        let d2 = emap.get(&dst).unwrap();
        g2.add_edge(*s2, *d2, ());
      }
    }
  }

  g2

  // graph.filter_map(|ni, n| {
  //   if nodes.contains(&ni) {
  //     Some(n.clone())
  //   } else {
  //     None
  //   }
  // }, |_ei, _e| Some(()))
}
