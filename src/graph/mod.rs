use std::path::Path;
use std::fs::File;
use zstd::Encoder;

use serde::{Serialize, Deserialize};
use petgraph::{Graph, Undirected};
use petgraph::graph::DefaultIx;
use petgraph::graph::NodeIndex;

use anyhow::Result;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BookID {
  pub code: i32,
  pub label: Option<String>
}
pub type IdGraph = Graph<BookID, (), Undirected>;
pub type IdNode = NodeIndex<DefaultIx>;

mod sources;
mod load;

pub use load::construct_graph;

/// Save a graph to a compressed, encoded file.
pub fn save_graph<P: AsRef<Path>>(graph: &IdGraph, path: P) -> Result<()> {
  let file = File::create(path)?;
  let mut out = Encoder::new(file, 9)?;
  rmp_serde::encode::write(&mut out, &graph)?;
  Ok(())
}
