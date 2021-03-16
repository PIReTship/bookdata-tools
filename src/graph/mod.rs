use petgraph::{Graph, Undirected};
use petgraph::graph::DefaultIx;
use petgraph::graph::NodeIndex;


pub type IdGraph = Graph<i32, (), Undirected>;
pub type IdNode = NodeIndex<DefaultIx>;

mod sources;
mod load;

pub use load::load_graph;

