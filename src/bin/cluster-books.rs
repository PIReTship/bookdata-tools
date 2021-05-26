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

#[derive(TableRow, Debug)]
struct ClusterRec {
  isbn_id: i32,
  cluster: i32
}

#[derive(TableRow, Debug)]
struct ClusterStat {
  cluster: i32,
  n_isbns: u32
}

#[tokio::main]
pub async fn main() -> Result<()> {
  let opts = ClusterBooks::from_args();
  opts.common.init()?;

  let graph = load_graph().await?;

  info!("computing connected components");
  let clusters = tarjan_scc(&graph);
  let msize = clusters.iter().map(|v| v.len()).max().unwrap_or_default();

  info!("computed {} clusters, largest has {} nodes", clusters.len(), msize);

  let mut ic_w = TableWriter::open("book-links/isbn-clusters.parquet")?;
  let mut cs_w = TableWriter::open("book-links/cluster-stats.parquet")?;

  for ci in 0..clusters.len() {
    let verts = &clusters[ci];
    let vids: Vec<_> = verts.iter().map(|v| {
      *(graph.node_weight(*v).unwrap())
    }).collect();
    let code = vids.iter().min().unwrap();
    let cluster = *code;
    cs_w.write_object(ClusterStat {
      cluster, n_isbns: vids.len() as u32
    })?;
    for v in &vids {
      if let Some(id) = NS_ISBN.from_code(*v) {
        ic_w.write_object(ClusterRec {
          cluster, isbn_id: id,
        })?;
      }
    }
  }

  ic_w.finish()?;
  cs_w.finish()?;

  Ok(())
}
