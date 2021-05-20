//! Support for numeric indexes for RDF nodes.
use std::path::Path;

use log::*;
use thiserror::Error;
use anyhow::Result;
use lazy_static::lazy_static;

use hashbrown::HashMap;
use uuid::Uuid;

use crate as bookdata;
use crate::io::ObjectWriter;
use crate::arrow::*;
use super::model::Node;

type Id = i32;

const NS_BLANK_URL: &'static str = "https://bookdata.piret.info/blank-ns";
lazy_static! {
  static ref NAMESPACE_BLANK: Uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, NS_BLANK_URL.as_bytes());
}

/// Error codes reported by the node index.
#[derive(Error, Debug)]
pub enum NodeIndexError {
  #[error("The index is read-only and the node is not saved.")]
  ReadOnlyIndex,
}

/// Data format for saving index rows
#[derive(Debug, TableRow)]
struct IndexRow {
  id: Id,
  uuid: Uuid,
  iri: String
}

/// Index for looking up node IDs from IRIs or blank labels.
///
/// An index can have a *backing file*, in which case it will write named
/// nodes and their identifiers to the file.  It can also be loaded from a
/// file, in which case it will be **read-only** (no new named identifiers
/// can be added).  Blank node labels are **not** saved in the file --- they
/// are generated afresh when the index is used, and can always be generated
/// even with a read-only node index.
///
/// Blank nodes resolve to negative identifiers, so there is no overlap between
/// blank and named node identifiers. 0 is never returned.
pub struct NodeIndex {
  named: HashMap<Uuid,Id>,
  blank: HashMap<Uuid,Id>,
  readonly: bool,
  writer: Option<TableWriter<IndexRow>>
}

impl NodeIndex {
  /// Create an empty in-memory node index.
  pub fn new_in_memory() -> NodeIndex {
    NodeIndex {
      named: HashMap::new(),
      blank: HashMap::new(),
      readonly: false,
      writer: None
    }
  }

  /// Create an empty index with a backing file.
  pub fn new_with_file<P: AsRef<Path>>(path: P) -> Result<NodeIndex> {
    let writer = TableWriter::open(path)?;
    Ok(NodeIndex {
      named: HashMap::new(),
      blank: HashMap::new(),
      readonly: false,
      writer: Some(writer)
    })
  }

  /// Get an ID for a named node.
  pub fn iri_id(&mut self, iri: &str) -> Result<Id> {
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, iri.as_bytes());
    if let Some(id) = self.named.get(&uuid) {
      Ok(*id as Id)
    } else if self.readonly {
      Err(NodeIndexError::ReadOnlyIndex.into())
    } else {
      let id = (self.named.len() + 1) as Id;
      self.named.insert(uuid.clone(), id);
      if let Some(w) = &mut self.writer {
        w.write_object(IndexRow {
          id, uuid, iri: iri.to_owned()
        })?;
      }
      Ok(id as Id)
    }
  }

  /// Get an ID for a blank node.
  pub fn blank_id(&mut self, label: &str) -> Result<Id> {
    let uuid = Uuid::new_v5(&NAMESPACE_BLANK, label.as_bytes());
    let next_id = -((self.blank.len() + 1) as Id);
    Ok(*self.blank.entry(uuid).or_insert(next_id))
  }

  /// Get an ID for a node.
  pub fn node_id(&mut self, node: &Node) -> Result<Id> {
    match node {
      Node::Named(iri) => self.iri_id(iri),
      Node::Blank(label) => self.blank_id(label)
    }
  }

  /// Close the index and make it read-only.
  pub fn finish(&mut self) -> Result<()> {
    if !self.readonly {
      info!("finished index with {} named and {} blank nodes",
            self.named.len(), self.blank.len());
      self.readonly = true;
      if let Some(writer) = self.writer.take() {
        writer.finish()?;
      }
    }
    Ok(())
  }
}
