use serde::Deserialize;
use petgraph::{Graph, Undirected};
use petgraph::graph::DefaultIx;
use petgraph::graph::NodeIndex;

#[derive(Deserialize, Debug, Clone)]
pub struct BookID {
  pub code: i32,
  pub label: Option<String>
}
pub type IdGraph = Graph<BookID, (), Undirected>;
pub type IdNode = NodeIndex<DefaultIx>;

mod sources;
mod load;

pub use load::load_graph;

