//! Scan the nodes in an RDF ntriples file.
use std::io::prelude::*;

use bookdata::prelude::*;
use bookdata::io::compress::open_solo_zip;
use bookdata::ids::index::IdIndex;

use ntriple::parser::quiet_line;
use ntriple::{Triple, Subject, Predicate, Object};

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

  fn scan_file(&mut self, path: &Path) -> Result<usize> {
    let (read, pb) = open_solo_zip(path)?;
    let _pbl = set_progress(&pb);
    let mut lno = 0;
    let mut nrecs = 0;
    for line in read.lines() {
      let line = line?;
      lno += 1;
      match quiet_line(&line) {
        Ok(Some(tr)) => {
          nrecs += 1;
          self.scan_triple(tr)?;
        },
        Ok(None) => (),
        Err(ref e) => {
          error!("error on line {}: {:?}", lno, e);
          error!("invalid line contained: {}", line);
        }
      };
    }

    Ok(nrecs)
  }

  fn scan_triple(&mut self, triple: Triple) -> Result<()> {
    self.subj_id(&triple.subject)?;
    self.pred_id(&triple.predicate)?;
    self.obj_id(&triple.object)?;
    Ok(())
  }

  fn node_id(&mut self, iri: &str) -> Result<i32> {
    // let uuid = Uuid::new_v5(&uuid::NAMESPACE_URL, iri);
    // self.node_sink.save(&uuid, iri)?;
    let id = self.iri_index.intern(iri.to_string());
    Ok(id.try_into()?)
  }

  fn blank_id(&mut self, key: &str) -> Result<i32> {
    // let uuid = Uuid::new_v5(&self.blank_ns, key);
    let id = self.blank_index.intern(key.to_string()) as i64;
    let id = -id;
    Ok(id.try_into()?)
  }

  /// Generate an ID for an RDF subject.
  pub fn subj_id(&mut self, sub: &Subject) -> Result<i32> {
    match sub {
      Subject::IriRef(iri) => self.node_id(iri),
      Subject::BNode(key) => self.blank_id(key)
    }
  }

  /// Generate an ID for an RDF predicate.
  pub fn pred_id(&mut self, pred: &Predicate) -> Result<i32> {
    match pred {
      Predicate::IriRef(iri) => self.node_id(iri)
    }
  }

  /// Generate an ID for an RDF object.
  pub fn obj_id(&mut self, obj: &Object) -> Result<Option<i32>> {
    match obj {
      Object::IriRef(iri) => self.node_id(iri).map(Some),
      Object::BNode(key) => self.blank_id(key).map(Some),
      Object::Lit(_) => Ok(None)
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
