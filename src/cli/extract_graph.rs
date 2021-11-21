//! Graph extraction program.
use std::convert::From;
use std::path::PathBuf;

use crate::prelude::*;
use crate::graph::{load_graph, save_gml};

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

impl Command for ExtractGraph {
  fn exec(&self) -> Result<()> {
    let opts = ExtractGraph::from_args();
    opts.common.init()?;

    let path = match &opts.graph_file {
      Some(p) => p.clone(),
      None => PathBuf::from("book-links/book-graph.mp.zst")
    };

    info!("loading graph from {}", path.to_string_lossy());
    let mut graph = load_graph(path)?;

    if let Some(c) = opts.cluster {
      graph.retain_nodes(|g, n| {
        let book = g.node_weight(n).unwrap();
        book.cluster == c
      });
    }

    info!("filtered graph to {} nodes", graph.node_count());

    if let Some(outf) = opts.out_file {
      save_gml(&graph, &outf)?;
    }

    Ok(())
  }
}
