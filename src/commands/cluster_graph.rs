use std::io::prelude::*;
use std::path::PathBuf;

use structopt::StructOpt;
use postgres::Connection;
use anyhow::Result;
use sha1::Sha1;
use log::*;
use petgraph::algo::tarjan_scc;

use crate::db::{DbOpts, CopyRequest};
use crate::tracking::StageOpts;
use crate::graph::{load_graph, IdGraph, IdNode};
use crate::io::HashWrite;
use crate::codes::NS_ISBN;
use super::Command;

/// Cluster graph nodes for ISBN clusters
#[derive(StructOpt, Debug)]
#[structopt(name="cluster-graph")]
pub struct ClusterGraph {
  #[structopt(flatten)]
  db: DbOpts,

  #[structopt(flatten)]
  stage: StageOpts,

  // The file to write the full ISBN graph.
  #[structopt(short="-o", long="out-file")]
  out_file: Option<PathBuf>
}


impl Command for ClusterGraph {
  fn exec(self) -> Result<()> {
    let db = self.db.open()?;
    let mut stage = self.stage.begin_stage(&db)?;

    info!("loading graph");
    let graph = load_graph(&db)?;
    writeln!(stage, "{} VERTICES", graph.node_count())?;
    writeln!(stage, "{} EDGES", graph.edge_count())?;
    info!("finished loading graph");

    info!("computing SCCs");
    let clusters = tarjan_scc(&graph);
    writeln!(stage, "{} CLUSTERS", clusters.len())?;
    info!("computed {} book clusters", clusters.len());

    reset_table(&db)?;
    let hash = write_clusters(&graph, &clusters, &self.db)?;
    index_table(&db)?;
    let digest = hash.hexdigest();
    writeln!(stage, "OUTPUT isbn_cluster {}", digest)?;
    stage.end(&Some(digest))?;

    Ok(())
  }
}

fn reset_table(db: &Connection) -> Result<()> {
  info!("dropping table");
  db.execute("DROP TABLE IF EXISTS isbn_cluster CASCADE", &[])?;
  info!("creating table");
  db.execute("
    CREATE TABLE isbn_cluster (
      isbn_id INTEGER NOT NULL,
      cluster INTEGER NOT NULL
    )
  ", &[])?;
  Ok(())
}

fn index_table(db: &Connection) -> Result<()> {
  info!("indexing tables");
  db.execute("ALTER TABLE isbn_cluster ADD PRIMARY KEY (isbn_id)", &[])?;
  db.execute("CREATE INDEX isbn_cluster_idx ON isbn_cluster (cluster)", &[])?;
  db.execute("ANALYZE isbn_cluster", &[])?;
  Ok(())
}

fn write_clusters(g: &IdGraph, clusters: &Vec<Vec<IdNode>>, db: &DbOpts) -> Result<Sha1> {
  let req = CopyRequest::new(db, "isbn_cluster")?;
  let out = req.open()?;
  let mut hash = Sha1::new();
  let mut out = HashWrite::create(out, &mut hash);

  info!("writing clusters");

  for ci in 0..clusters.len() {
    let verts = &clusters[ci];
    let vids: Vec<_> = verts.iter().map(|v| {
      *(g.node_weight(*v).unwrap())
    }).collect();
    let code = vids.iter().min().unwrap();
    for v in &vids {
      if let Some(id) = NS_ISBN.from_code(*v) {
        writeln!(out, "{}\t{}", id, code)?;
      }
    }
  }

  Ok(hash)
}
