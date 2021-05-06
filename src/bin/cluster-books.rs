use tokio;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::graph::load_graph;
use bookdata::ids::codes::NS_ISBN;

use petgraph::algo::tarjan_scc;

/// Run the book clustering algorithm.
#[derive(StructOpt, Debug)]
#[structopt(name="cluster-books")]
pub struct ClusterBooks {
  #[structopt(flatten)]
  common: CommonOpts,
}

#[derive(TableRow)]
struct ClusterRec {
  isbn_id: i32,
  cluster: i32
}

#[tokio::main]
pub async fn main() -> Result<()> {
  let opts = ClusterBooks::from_args();
  opts.common.init()?;

  let graph = load_graph().await?;

  info!("computing connected components");
  let clusters = tarjan_scc(&graph);

  info!("writing clusters");
  let mut writer = TableWriter::open("book-links/isbn-clusters.parquet")?;

  for ci in 0..clusters.len() {
    let verts = &clusters[ci];
    let vids: Vec<_> = verts.iter().map(|v| {
      *(graph.node_weight(*v).unwrap())
    }).collect();
    let code = vids.iter().min().unwrap();
    for v in &vids {
      if let Some(id) = NS_ISBN.from_code(*v) {
        writer.write_object(ClusterRec {
          isbn_id: id,
          cluster: *code
        })?;
      }
    }
  }

  writer.finish()?;

  Ok(())
}
