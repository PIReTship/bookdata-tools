//! Book clustering command.
use std::thread::{scope, Scope, ScopedJoinHandle};

use petgraph::algo::kosaraju_scc;

use crate::graph::model::*;
use crate::graph::*;
use crate::prelude::*;

/// Run the book clustering algorithm.
#[derive(Args, Debug)]
#[command(name = "cluster-books")]
pub struct ClusterBooks {
    #[arg(long = "save-graph")]
    save_graph: Option<PathBuf>,
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

        scope(|scope| -> Result<()> {
            let sthread = self.maybe_save_graph(scope, &graph);

            save_graph_cluster_data(&graph, clusters)?;

            if let Some(h) = sthread {
                info!("waiting for background save to finish");
                let br = h.join().expect("thread join failed");
                br?;
            }
            Ok(())
        })?;

        Ok(())
    }
}

impl ClusterBooks {
    fn maybe_save_graph<'scope, 'env>(
        &'env self,
        scope: &'scope Scope<'scope, 'env>,
        graph: &'env IdGraph,
    ) -> Option<ScopedJoinHandle<'scope, Result<()>>> {
        if let Some(gf) = &self.save_graph {
            Some(scope.spawn(move || {
                info!("saving graph to {} (in background)", gf.display());
                let res = save_graph(&graph, gf);
                if let Err(e) = &res {
                    error!("error saving graph: {}", e);
                }
                res
            }))
        } else {
            None
        }
    }
}
