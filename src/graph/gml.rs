use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;
use std::path::Path;

use anyhow::Result;
use log::*;
use petgraph::visit::*;

use crate::ids::codes::ns_of_book_code;

use super::{IdGraph, IdNode};

fn gml_begin<W: Write>(w: &mut W) -> Result<()> {
    writeln!(w, "graph [")?;
    Ok(())
}

fn gml_end<W: Write>(w: &mut W) -> Result<()> {
    writeln!(w, "]")?;
    Ok(())
}

fn gml_node<W: Write>(w: &mut W, graph: &IdGraph, v: IdNode) -> Result<()> {
    let node = graph.node_weight(v).unwrap();
    writeln!(w, "  node [")?;
    writeln!(w, "    id {}", node.code)?;
    let ns = ns_of_book_code(node.code).unwrap();
    writeln!(w, "    namespace \"{}\"", ns.name())?;
    if let Some(ref l) = node.label {
        writeln!(w, "    label \"{}\"", l)?;
    }
    writeln!(w, "  ]")?;
    Ok(())
}

fn gml_edge<W: Write>(w: &mut W, graph: &IdGraph, sv: IdNode, dv: IdNode) -> Result<()> {
    let src = graph.node_weight(sv).unwrap();
    let dst = graph.node_weight(dv).unwrap();
    writeln!(w, "  edge [")?;
    writeln!(w, "    source {}", src.code)?;
    writeln!(w, "    target {}", dst.code)?;
    writeln!(w, "  ]")?;
    Ok(())
}

/// Save a graph to a GML file.
pub fn save_gml<P: AsRef<Path>>(graph: &IdGraph, path: P) -> Result<()> {
    info!("saving graph to {}", path.as_ref().to_string_lossy());
    let out = File::create(path)?;
    let mut out = BufWriter::new(out);
    gml_begin(&mut out)?;
    for n in graph.node_indices() {
        gml_node(&mut out, graph, n)?;
    }
    for e in graph.edge_references() {
        gml_edge(&mut out, graph, e.source(), e.target())?;
    }
    gml_end(&mut out)?;
    Ok(())
}
