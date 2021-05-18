//! Scan the nodes in an RDF ntriples file.
use bookdata::prelude::*;
use bookdata::io::compress::open_solo_zip;
use bookdata::ids::index::IdIndex;

use oxigraph::model::*;
use oxigraph::io::GraphFormat;
use oxigraph::io::read::GraphParser;

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
    let (read, pb) = open_solo_zip(path)?;
    let _pbl = set_progress(&pb);
    let parser = GraphParser::from_format(GraphFormat::NTriples);
    for triple in parser.read_triples(read)? {
      let triple = triple?;
      self.scan_triple(triple)?;
    }

    Ok(())
  }

  fn scan_triple(&mut self, triple: Triple) -> Result<()> {
    self.subj_id(triple.subject)?;
    self.pred_id(triple.predicate)?;
    self.obj_id(triple.object)?;
    Ok(())
  }

  fn node_id(&mut self, node: NamedNode) -> Result<i32> {
    // let uuid = Uuid::new_v5(&uuid::NAMESPACE_URL, node);
    // self.node_sink.save(&uuid, node)?;
    let id = self.iri_index.intern(node.into_string());
    Ok(id.try_into()?)
  }

  fn blank_id(&mut self, node: BlankNode) -> Result<i32> {
    // let uuid = Uuid::new_v5(&self.blank_ns, key);
    let id = self.blank_index.intern(node.into_string()) as i64;
    let id = -id;
    Ok(id.try_into()?)
  }

  /// Generate an ID for an RDF subject.
  pub fn subj_id(&mut self, sub: NamedOrBlankNode) -> Result<i32> {
    match sub {
      NamedOrBlankNode::NamedNode(node) => self.node_id(node),
      NamedOrBlankNode::BlankNode(node) => self.blank_id(node)
    }
  }

  /// Generate an ID for an RDF predicate.
  pub fn pred_id(&mut self, pred: NamedNode) -> Result<i32> {
    self.node_id(pred)
  }

  /// Generate an ID for an RDF object.
  pub fn obj_id(&mut self, obj: Term) -> Result<Option<i32>> {
    match obj {
      Term::NamedNode(node) => self.node_id(node).map(Some),
      Term::BlankNode(node) => self.blank_id(node).map(Some),
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
