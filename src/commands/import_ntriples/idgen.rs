/// This module contains code for generating UUIDs for RDF nodes and identifiers.

use anyhow::Result;
use ntriple::{Subject, Predicate, Object};
use uuid::Uuid;

use super::sink::NodeSink;

pub struct IdGenerator {
  blank_ns: Uuid,
  node_sink: NodeSink
}

#[derive(Debug)]
pub enum Target<'a> {
  Node(Uuid),
  Literal(&'a ntriple::Literal)
}

impl IdGenerator {
  /// Create a new ID generator.  It takes a node sink and a name to be used for generating
  /// UUIDs for 'blank' nodes; this should be different for each file read.
  pub fn create(nodes: NodeSink, name: &str) -> IdGenerator {
    let ns_ns = Uuid::new_v5(&uuid::NAMESPACE_URL, "https://boisestate.github.io/bookdata/ns/blank");
    let blank_ns = Uuid::new_v5(&ns_ns, name);
    IdGenerator {
      blank_ns: blank_ns,
      node_sink: nodes
    }
  }

  fn node_id(&mut self, iri: &str) -> Result<Uuid> {
    let uuid = Uuid::new_v5(&uuid::NAMESPACE_URL, iri);
    self.node_sink.save(&uuid, iri)?;
    Ok(uuid)
  }

  fn blank_id(&self, key: &str) -> Result<Uuid> {
    let uuid = Uuid::new_v5(&self.blank_ns, key);
    Ok(uuid)
  }

  /// Generate an ID for an RDF subject.
  pub fn subj_id(&mut self, sub: &Subject) -> Result<Uuid> {
    match sub {
      Subject::IriRef(iri) => self.node_id(iri),
      Subject::BNode(key) => self.blank_id(key)
    }
  }

  /// Generate an ID for an RDF predicate.
  pub fn pred_id(&mut self, pred: &Predicate) -> Result<Uuid> {
    match pred {
      Predicate::IriRef(iri) => self.node_id(iri)
    }
  }

  /// Generate an ID for an RDF object.
  pub fn obj_id<'a>(&mut self, obj: &'a Object) -> Result<Target<'a>> {
    match obj {
      Object::IriRef(iri) => self.node_id(iri).map(Target::Node),
      Object::BNode(key) => self.blank_id(key).map(Target::Node),
      Object::Lit(l) => Ok(Target::Literal(&l))
    }
  }
}
