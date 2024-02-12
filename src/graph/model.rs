use std::fs::File;

use anyhow::{anyhow, Result};
use log::*;
use parquet_derive::{ParquetRecordReader, ParquetRecordWriter};
use serde::Serialize;

use super::{BookID, IdGraph, IdNode};
use crate::arrow::TableWriter;
use crate::ids::codes::{ns_of_book_code, NS_ISBN};
use crate::io::object::ObjectWriter;
use crate::util::logging::item_progress;

const ISBN_CLUSTER_PATH: &str = "book-links/isbn-clusters.parquet";
const GRAPH_NODE_PATH: &str = "book-links/cluster-graph-nodes.parquet";
const GRAPH_EDGE_PATH: &str = "book-links/cluster-graph-edges.parquet";
const CLUSTER_STATS_PATH: &str = "book-links/cluster-stats.parquet";
const CLUSTER_METRICS_PATH: &str = "book-links/cluster-metrics.json";

#[derive(ParquetRecordWriter, ParquetRecordReader, Debug)]
pub struct ISBNClusterRec {
    pub isbn: String,
    pub isbn_id: i32,
    pub cluster: i32,
}

#[derive(ParquetRecordWriter, Debug)]
pub struct ClusterCode {
    pub book_code: i32,
    pub cluster: i32,
    pub node_type: String,
    pub label: Option<String>,
}

#[derive(ParquetRecordWriter, Debug)]
pub struct GraphEdge {
    pub src: i32,
    pub dst: i32,
}

#[derive(ParquetRecordWriter, Debug, Default)]
pub struct ClusterStat {
    pub cluster: i32,
    pub n_nodes: u32,
    pub n_isbns: u32,
    pub n_loc_recs: u32,
    pub n_ol_editions: u32,
    pub n_ol_works: u32,
    pub n_gr_books: u32,
    pub n_gr_works: u32,
}

#[derive(Serialize, Debug)]
struct ClusteringStatistics {
    clusters: usize,
    largest: usize,
    max_isbns: usize,
}

impl ClusterStat {
    /// Create a cluster statistics object from a list of books codes.
    pub fn create(cluster: i32, nodes: &Vec<&BookID>) -> ClusterStat {
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

pub fn save_graph_cluster_data(graph: &IdGraph, clusters: Vec<Vec<IdNode>>) -> Result<()> {
    let mut ic_w = TableWriter::open(ISBN_CLUSTER_PATH)?;

    let mut n_w = TableWriter::open(GRAPH_NODE_PATH)?;
    let mut cs_w = TableWriter::open(CLUSTER_STATS_PATH)?;

    let mut m_size = 0;
    let mut m_id = 0;
    let mut m_isbns = 0;

    info!("writing graph nodes");
    let pb = item_progress(clusters.len(), "clusters");
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
        let mut n_isbns = 0;
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
                    isbn: v
                        .label
                        .clone()
                        .ok_or_else(|| anyhow!("graph node missing ISBN label"))?,
                })?;
                n_isbns += 1;
            }
        }
        if n_isbns > m_isbns {
            m_isbns = n_isbns;
        }
        pb.tick();
    }

    ic_w.finish()?;
    n_w.finish()?;
    cs_w.finish()?;
    pb.finish_and_clear();

    info!("largest cluster {} has {} nodes", m_id, m_size);

    info!("writing graph edges");
    let mut e_w = TableWriter::open(GRAPH_EDGE_PATH)?;
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
        max_isbns: m_isbns,
    };
    let statf = File::create(CLUSTER_METRICS_PATH)?;
    serde_json::to_writer(statf, &stats)?;

    Ok(())
}
