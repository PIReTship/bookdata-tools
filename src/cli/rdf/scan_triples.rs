//! Scan the nodes in an RDF ntriples file.
use crate::prelude::*;
use crate::io::{LineProcessor, ObjectWriter};
use crate::rdf::model::*;
use crate::rdf::nodeindex::NodeIndex;
use crate::rdf::nsmap::NSMap;
use crate::arrow::*;

use crate as bookdata;

use crate::cli::Command;

#[derive(StructOpt, Debug)]
#[structopt(name="rdf-scan-triples")]
/// Scan and import RDF triples.
pub struct ScanTriples {
  /// Use a namespace map file.
  #[structopt(short="m", long="ns-map")]
  nsmap: Option<PathBuf>,

  /// Write named node IDs to an output file.
  #[structopt(short="N", long="save-nodes")]
  node_file: PathBuf,

  /// Write triples to an output file
  #[structopt(short="T", long="save-triples")]
  trip_file: PathBuf,

  /// Write literals to an output file
  #[structopt(short="L", long="save-literals")]
  lit_file: PathBuf,

  /// Input files
  #[structopt(name = "FILE", parse(from_os_str))]
  infiles: Vec<PathBuf>,
}

#[derive(TableRow)]
struct TripleRow {
  subject: i32,
  predicate: i32,
  object: i32,
}

#[derive(TableRow)]
struct LitRow {
  subject: i32,
  predicate: i32,
  literal: String,
}

struct TripleScanner {
  trip_out: TableWriter<TripleRow>,
  lit_out: TableWriter<LitRow>,
  nodes: NodeIndex,
  count: usize,
}

impl TripleScanner {
  fn open(opts: &ScanTriples) -> Result<TripleScanner> {
    let mut nodes = NodeIndex::new_with_file(&opts.node_file)?;
    if let Some(path) = &opts.nsmap {
      let map = NSMap::load(path)?;
      nodes.set_nsmap(&map);
    }
    let trip_out = TableWriter::open(&opts.trip_file)?;
    let lit_out = TableWriter::open(&opts.lit_file)?;
    Ok(TripleScanner {
      trip_out, lit_out, nodes, count: 0
    })
  }

  fn scan_file(&mut self, path: &Path) -> Result<()> {
    info!("scanning file {}", path.to_string_lossy());
    let proc = LineProcessor::open_solo_zip(path)?;
    for triple in proc.records() {
      let triple: Triple = triple?;
      if !triple.is_empty() {
        self.scan_triple(triple)?;
      }
    }

    Ok(())
  }

  fn scan_triple(&mut self, triple: Triple) -> Result<()> {
    self.count += 1;
    let subject = self.nodes.node_id(&triple.subject)?;
    let predicate = self.nodes.iri_id(&triple.predicate)?;

    match triple.object {
      Term::Node(node) => {
        let object = self.nodes.node_id(&node)?;
        self.trip_out.write_object(TripleRow {
          subject, predicate, object
        })?;
      },
      Term::Literal(lit) => {
        self.lit_out.write_object(LitRow {
          subject, predicate, literal: lit.value
        })?;
      }
    }

    Ok(())
  }

  fn finish(mut self) -> Result<usize> {
    info!("finalizing outputs");
    self.nodes.finish()?;
    self.trip_out.finish()?;
    self.lit_out.finish()?;
    Ok(self.count)
  }
}

impl Command for ScanTriples {
  fn exec(&self) -> Result<()> {
    let mut scanner = TripleScanner::open(&self)?;

    for file in &self.infiles {
      scanner.scan_file(file.as_ref())?;
    }

    let n = scanner.finish()?;
    info!("saved {} triples from {} files", n, self.infiles.len());

    Ok(())
  }
}
