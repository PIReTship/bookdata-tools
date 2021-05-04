use std::path::Path;
use std::fs::File;
use std::collections::HashMap;
use std::hash::Hash;
use std::borrow::Borrow;

use log::*;
use anyhow::Result;
use arrow::datatypes::*;
use parquet::arrow::arrow_to_parquet_schema;
use parquet::file::reader::SerializedFileReader;
use parquet::record::reader::RowIter;
use parquet::record::RowAccessor;

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

  /// Look up the ID for a key if it is present.
  pub fn lookup<Q>(&mut self, key: &Q) -> Option<u32> where K: Borrow<Q>, Q: Hash + Eq + ?Sized {
    self.map.get(key).map(|i| *i)
  }
}

impl IdIndex<String> {
  /// Load from a Parquet file, with a standard configuration.
  ///
  /// This assumes the Parquet file has the following columns:
  ///
  /// - `key`, of type `String`, storing the keys
  /// - `id`, of type `u32`, storing the IDs
  pub fn load_standard<P: AsRef<Path>>(path: P) -> Result<IdIndex<String>> {
    let schema = Schema::new(vec![
      Field::new("id", DataType::UInt32, false),
      Field::new("key", DataType::Utf8, false),
    ]);
    let pqs = arrow_to_parquet_schema(&schema)?;
    let proj = pqs.root_schema();

    let path_str = path.as_ref().to_string_lossy();
    info!("reading index from file {}", path_str);
    let file = File::open(path.as_ref())?;
    let read = SerializedFileReader::new(file)?;
    let read = RowIter::from_file(Some(proj.clone()), &read)?;
    let mut map = HashMap::new();

    for row in read {
      let id = row.get_uint(0)?;
      let key = row.get_string(1)?;
      map.insert(key.clone(), id);
    }

    info!("read {} keys from {}", map.len(), path_str);

    Ok(IdIndex {
      map
    })
  }
}
