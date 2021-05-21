//! Routines for working with UUIDs.
use uuid::Uuid;

/// A factory for named-based UUIDs.
struct NameUUIDFactory {
  namespace: Uuid
}

impl NameUUIDFactory {
  /// Create a factory with a namespace.
  pub fn new(ns: &Uuid) -> NameUUIDFactory {
    NameUUIDFactory {
      namespace: ns.clone()
    }
  }

  /// Make a new UUID.
  pub fn make_uuid(&self, string: &str) -> Uuid {
    Uuid::new_v5(&self.namespace, string.as_bytes())
  }
}
