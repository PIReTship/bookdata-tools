use std::path::Path;
use std::fs::File;
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
