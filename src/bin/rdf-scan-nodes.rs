//! Scan the nodes in an RDF ntriples file.
use bookdata::prelude::*;
use bookdata::io::LineProcessor;
use bookdata::ids::index::IdIndex;
use bookdata::rdf::model::*;

#[derive(StructOpt, Debug)]
#[structopt(name="rdf-scan-nodes")]
/// Scan the nodes in one or more RDF triples files.
pub struct ScanNodes {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Input file
  #[structopt(name = "FILE", parse(from_os_str))]
  infiles: Vec<PathBuf>,
}

struct ScanContext {
  iri_index: IdIndex<String>,
  blank_index: IdIndex<String>
}

impl ScanContext {
  fn new() -> ScanContext {
    ScanContext {
      iri_index: IdIndex::new(),
      blank_index: IdIndex::new()
    }
  }

  fn scan_file(&mut self, path: &Path) -> Result<()> {
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
    self.node_id(triple.subject)?;
    self.iri_id(triple.predicate)?;
    self.term_id(triple.object)?;
    Ok(())
  }

  fn iri_id(&mut self, node: String) -> Result<i32> {
    // let uuid = Uuid::new_v5(&uuid::NAMESPACE_URL, node);
    // self.node_sink.save(&uuid, node)?;
    let id = self.iri_index.intern(node);
    Ok(id.try_into()?)
  }

  fn blank_id(&mut self, node: String) -> Result<i32> {
    // let uuid = Uuid::new_v5(&self.blank_ns, key);
    let id = self.blank_index.intern(node) as i64;
    let id = -id;
    Ok(id.try_into()?)
  }

  /// Generate an ID for an RDF node.
  pub fn node_id(&mut self, sub: Node) -> Result<i32> {
    match sub {
      Node::Named(node) => self.iri_id(node),
      Node::Blank(node) => self.blank_id(node)
    }
  }

  /// Generate an ID for an RDF term.
  pub fn term_id(&mut self, obj: Term) -> Result<Option<i32>> {
    match obj {
      Term::Node(node) => self.node_id(node).map(Some),
      Term::Literal(_) => Ok(None)
    }
  }
}

pub fn main() -> Result<()> {
  let opts = ScanNodes::from_args();
  opts.common.init()?;

  let mut scan = ScanContext::new();
  for file in opts.infiles {
    scan.scan_file(file.as_ref())?;
  }

  let iri_len: usize = scan.iri_index.keys().map(|k| k.len()).sum();
  info!("scanned {} named nodes ({} total length)", scan.iri_index.len(), iri_len);
  let blank_len:usize  = scan.blank_index.keys().map(|k| k.len()).sum();
  info!("scanned {} blank nodes ({} total length)", scan.blank_index.len(), blank_len);

  Ok(())
}
