use std::collections::HashMap;
use std::hash::Hash;

/// Index identifiers from a data type
pub struct IdIndex<K> {
  map: HashMap<K,u32>
}

impl <K> IdIndex<K> where K: Eq + Hash {
  /// Create a new index.
  pub fn new() -> IdIndex<K> {
    IdIndex {
      map: HashMap::new()
    }
  }

  /// Get the index length
  pub fn len(&self) -> usize {
    self.map.len()
  }

  /// Get the ID for a key, adding it to the index if needed.
  pub fn intern(&mut self, key: K) -> u32 {
    let n = self.map.len() as u32;
    *self.map.entry(key).or_insert(n + 1)
  }
}
