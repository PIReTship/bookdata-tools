//! Book clustering command.
use std::fs::File;
use std::sync::Arc;
use std::thread::spawn;

use crate::arrow::*;
use crate::graph::*;
use crate::ids::codes::{ns_of_book_code, NS_ISBN};
use crate::prelude::*;

use petgraph::algo::kosaraju_scc;
use serde::Serialize;

/// Run the book clustering algorithm.
#[derive(Args, Debug)]
#[command(name = "cluster-books")]
pub struct ClusterBooks {
    #[arg(long = "save-graph")]
    save_graph: Option<PathBuf>,
}

#[derive(TableRow, Debug)]
struct ISBNClusterRec {
    isbn: Option<String>,
    isbn_id: i32,
    cluster: i32,
}

#[derive(TableRow, Debug)]
struct ClusterCode {
    book_code: i32,
    cluster: i32,
    node_type: String,
    label: Option<String>,
}

#[derive(TableRow, Debug)]
struct GraphEdge {
    src: i32,
    dst: i32,
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

#[derive(Serialize, Debug)]
struct ClusteringStatistics {
    clusters: usize,
    largest: usize,
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
                    _ => (),
                }
            }
        }

        cs
    }
}

impl Command for ClusterBooks {
    fn exec(&self) -> Result<()> {
        let cfg = load_config()?;
        let mut graph = construct_graph(&cfg)?;

        info!("computing connected components");
        let clusters = kosaraju_scc(&graph);

        info!("computed {} clusters", clusters.len());

        info!("adding cluster annotations");
        for ci in 0..clusters.len() {
            let verts = &clusters[ci];
            let vids: Vec<_> = verts
                .iter()
                .map(|v| graph.node_weight(*v).unwrap())
                .collect();
            let cluster = vids.iter().map(|b| b.code).min().unwrap();
            for v in verts {
                let node = graph.node_weight_mut(*v).unwrap();
                node.cluster = cluster;
            }
        }

        let graph = Arc::new(graph);
        let sthread = if let Some(gf) = &self.save_graph {
            let path = gf.clone();
            let g2 = graph.clone();
            Some(spawn(move || {
                info!("saving graph to {:?} (in background)", path);
                let res = save_graph(&g2, &path);
                if let Err(e) = &res {
                    error!("error saving graph: {}", e);
                }
                res
            }))
        } else {
            None
        };

        info!("preparing to write graph results");
        let mut ic_w = TableWriter::open("book-links/isbn-clusters.parquet")?;

        let mut n_w = TableWriter::open("book-links/cluster-graph-nodes.parquet")?;
        let mut cs_w = TableWriter::open("book-links/cluster-stats.parquet")?;

        let mut m_size = 0;
        let mut m_id = 0;

        for ci in 0..clusters.len() {
            let verts = &clusters[ci];
            let vids: Vec<_> = verts
                .iter()
                .map(|v| graph.node_weight(*v).unwrap())
                .collect();
            let cluster = vids.iter().map(|b| b.code).min().unwrap();
            if vids.len() > m_size {
                m_size = vids.len();
                m_id = cluster;
            }
            cs_w.write_object(ClusterStat::create(cluster, &vids))?;
            for v in &vids {
                n_w.write_object(ClusterCode {
                    cluster,
                    book_code: v.code,
                    node_type: ns_of_book_code(v.code).unwrap().name.to_string(),
                    label: v.label.clone(),
                })?;
                if let Some(id) = NS_ISBN.from_code(v.code) {
                    ic_w.write_object(ISBNClusterRec {
                        cluster,
                        isbn_id: id,
                        isbn: v.label.clone(),
                    })?;
                }
            }
        }

        ic_w.finish()?;
        n_w.finish()?;
        cs_w.finish()?;

        info!("largest cluster {} has {} nodes", m_id, m_size);

        info!("writing graph edges");
        let mut e_w = TableWriter::open("book-links/cluster-graph-edges.parquet")?;
        for e in graph.edge_indices() {
            let (s, d) = graph.edge_endpoints(e).unwrap();
            let src = graph.node_weight(s).unwrap().code;
            let dst = graph.node_weight(d).unwrap().code;
            e_w.write_object(GraphEdge { src, dst })?;
        }
        e_w.finish()?;

        info!("saving statistics");
        let stats = ClusteringStatistics {
            clusters: clusters.len(),
            largest: m_size,
        };
        let statf = File::create("book-links/cluster-metrics.json")?;
        serde_json::to_writer(statf, &stats)?;

        if let Some(h) = sthread {
            debug!("waiting on background thread");
            let br = h.join().expect("thread join failed");
            br?;
        }

        Ok(())
    }
}
