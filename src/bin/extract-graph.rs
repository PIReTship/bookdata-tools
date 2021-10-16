use std::convert::From;
use std::path::PathBuf;
use std::collections::HashSet;

use bookdata::prelude::*;
use bookdata::graph::{IdGraph, IdNode, load_graph, save_gml, filter_to_nodes};

use petgraph::algo::kosaraju_scc;

/// Extract a subgraph.
#[derive(StructOpt, Debug)]
#[structopt(name="extract-graph")]
pub struct ExtractGraph {
  #[structopt(flatten)]
  common: CommonOpts,

  #[structopt(long="graph-file")]
  graph_file: Option<PathBuf>,

  #[structopt(short="c", long="cluster")]
  cluster: Option<i32>,

  #[structopt(long="output", short="o")]
  out_file: Option<PathBuf>,
}

/// Search for a cluster containing a particular graph.
fn find_cluster(graph: &IdGraph, cc: &Vec<Vec<IdNode>>, code: i32) -> HashSet<IdNode> {
  info!("finding cluster {}", code);
  for c in cc {
    for ni in c {
      let book = graph.node_weight(*ni).unwrap();
      if book.code == code {
        return c.iter().map(|i| *i).collect();
      }
    }
  }

  return HashSet::new();
}

fn main() -> Result<()> {
  let opts = ExtractGraph::from_args();
  opts.common.init()?;

  let path = match &opts.graph_file {
    Some(p) => p.clone(),
    None => PathBuf::from("book-links/book-graph.mp.zst")
  };

  info!("loading graph from {}", path.to_string_lossy());
  let graph = load_graph(path)?;

  info!("getting connected components");
  let cc = kosaraju_scc(&graph);

  let nodes = if let Some(c) = opts.cluster {
    find_cluster(&graph, &cc, c)
  } else {
    HashSet::new()
  };

  info!("filtering graph to {} nodes", nodes.len());
  let subgraph = filter_to_nodes(&graph, &nodes);

  if let Some(outf) = opts.out_file {
    save_gml(&subgraph, &outf)?;
  }

  Ok(())
}
