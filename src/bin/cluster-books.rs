use tokio;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::graph::{BookID, construct_graph, save_graph};
use bookdata::ids::codes::{NS_ISBN, ns_of_book_code};

use petgraph::algo::kosaraju_scc;

/// Run the book clustering algorithm.
#[derive(StructOpt, Debug)]
#[structopt(name="cluster-books")]
pub struct ClusterBooks {
  #[structopt(flatten)]
  common: CommonOpts,
}

#[derive(TableRow, Debug)]
struct ISBNClusterRec {
  isbn: Option<String>,
  isbn_id: i32,
  cluster: i32
}

#[derive(TableRow, Debug)]
struct ClusterCode {
  cluster: i32,
  book_code: i32,
}

#[derive(TableRow, Debug, Default)]
struct ClusterStat {
  cluster: i32,
  n_nodes: u32,
  n_isbns: u32,
  n_loc_recs: u32,
  n_ol_editions: u32,
  n_ol_works: u32,
  n_gr_books: u32,
  n_gr_works: u32,
}

impl ClusterStat {
  /// Create a cluster statistics object from a list of books codes.
  fn create(cluster: i32, nodes: &Vec<&BookID>) -> ClusterStat {
    let mut cs = ClusterStat::default();
    cs.cluster = cluster;
    cs.n_nodes = nodes.len() as u32;
    for node in nodes {
      if let Some(ns) = ns_of_book_code(node.code) {
        match ns.name {
          "ISBN" => cs.n_isbns += 1,
          "LOC" => cs.n_loc_recs += 1,
          "OL-W" => cs.n_ol_works += 1,
          "OL-E" => cs.n_ol_editions += 1,
          "GR-W" => cs.n_gr_works += 1,
          "GR-B" => cs.n_gr_books += 1,
          _ => ()
        }
      }
    }

    cs
  }
}

#[tokio::main]
pub async fn main() -> Result<()> {
  let opts = ClusterBooks::from_args();
  opts.common.init()?;

  let graph = construct_graph().await?;

  info!("saving graph");
  save_graph(&graph, "book-links/book-graph.mp.zst")?;

  info!("computing connected components");
  let clusters = kosaraju_scc(&graph);
  let msize = clusters.iter().map(|v| v.len()).max().unwrap_or_default();

  info!("computed {} clusters, largest has {} nodes", clusters.len(), msize);

  let mut ic_w = TableWriter::open("book-links/isbn-clusters.parquet")?;

  let cc_wb = TableWriterBuilder::new();
  let mut cc_w = cc_wb.open("book-links/cluster-codes.parquet")?;

  let mut cs_w = TableWriter::open("book-links/cluster-stats.parquet")?;

  for ci in 0..clusters.len() {
    let verts = &clusters[ci];
    let vids: Vec<_> = verts.iter().map(|v| {
      graph.node_weight(*v).unwrap()
    }).collect();
    let cluster = vids.iter().map(|b| b.code).min().unwrap();
    cs_w.write_object(ClusterStat::create(cluster, &vids))?;
    for v in &vids {
      cc_w.write_object(ClusterCode {
        cluster, book_code: v.code
      })?;
      if let Some(id) = NS_ISBN.from_code(v.code) {
        ic_w.write_object(ISBNClusterRec {
          cluster, isbn_id: id, isbn: v.label.clone()
        })?;
      }
    }
  }

  ic_w.finish()?;
  cc_w.finish()?;
  cs_w.finish()?;

  Ok(())
}
