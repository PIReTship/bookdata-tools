use std::path::{Path};
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
use crate::io::ObjectWriter;
use crate::arrow::*;

#[cfg(test)]
use quickcheck::{Arbitrary,Gen};
#[cfg(test)]
use tempfile::tempdir;

use crate as bookdata;

/// Index identifiers from a data type
pub struct IdIndex<K> {
  map: HashMap<K,u32>
}

/// Internal struct for ID records.
#[derive(TableRow)]
struct IdRec {
  id: u32,
  key: String
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
  pub fn lookup<Q>(&self, key: &Q) -> Option<u32> where K: Borrow<Q>, Q: Hash + Eq + ?Sized {
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
    IdIndex::load(path, "id", "key")
  }

  /// Load from a Parquet file.
  ///
  /// This loads two columns from a Parquet file.  The ID column is expected to
  /// have type `UInt32` (or a type projectable to it), and the key column should
  /// be `Utf8`.
  pub fn load<P: AsRef<Path>>(path: P, id_col: &str, key_col: &str) -> Result<IdIndex<String>> {
    let schema = Schema::new(vec![
      Field::new(id_col, DataType::UInt32, false),
      Field::new(key_col, DataType::Utf8, false),
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

  /// Save to a Parquet file with the standard configuration.
  pub fn save_standard<P: AsRef<Path>>(&self, path: P) -> Result<()> {
    self.save(path, "id", "key")
  }

  /// Save to a Parquet file with the standard configuration.
  pub fn save<P: AsRef<Path>>(&self, path: P, id_col: &str, key_col: &str) -> Result<()> {
    let mut wb = TableWriterBuilder::new();
    wb.rename("id", id_col);
    wb.rename("key", key_col);
    let mut writer = wb.open(path)?;

    for (k, v) in &self.map {
      writer.write_object(IdRec {
        id: *v,
        key: k.clone()
      })?;
    }

    writer.finish()?;

    Ok(())
  }
}


#[test]
fn test_index_empty() {
  let index: IdIndex<String> = IdIndex::new();
  assert_eq!(index.len(), 0);
  assert!(index.lookup("bob").is_none());
}


#[test]
fn test_index_intern_one() {
  let mut index: IdIndex<String> = IdIndex::new();
  assert!(index.lookup("hackem muche").is_none());
  let id = index.intern("hackem muche".to_string());
  assert_eq!(id, 1);
  assert_eq!(index.lookup("hackem muche").unwrap(), 1);
}


#[test]
fn test_index_intern_two() {
  let mut index: IdIndex<String> = IdIndex::new();
  assert!(index.lookup("hackem muche").is_none());
  let id = index.intern("hackem muche".to_string());
  assert_eq!(id, 1);
  let id2 = index.intern("readme".to_string());
  assert_eq!(id2, 2);
  assert_eq!(index.lookup("hackem muche").unwrap(), 1);
}


#[test]
fn test_index_intern_twice() {
  let mut index: IdIndex<String> = IdIndex::new();
  assert!(index.lookup("hackem muche").is_none());
  let id = index.intern("hackem muche".to_string());
  assert_eq!(id, 1);
  let id2 = index.intern("hackem muche".to_string());
  assert_eq!(id2, 1);
  assert_eq!(index.len(), 1);
}

#[test]
fn test_index_save() -> Result<()> {
  let mut index: IdIndex<String> = IdIndex::new();
  let mut gen = Gen::new(100);
  for _i in 0..10000 {
    let key = String::arbitrary(&mut gen);
    let prev = index.lookup(&key);
    let id = index.intern(key);
    match prev {
      Some(i) => assert_eq!(id, i),
      None => assert_eq!(id as usize, index.len())
    };
  }

  let dir = tempdir()?;
  let pq = dir.path().join("index.parquet");
  index.save_standard(&pq).expect("save error");

  let i2 = IdIndex::load_standard(&pq).expect("load error");
  assert_eq!(i2.len(), index.len());
  for (k, v) in &index.map {
    let v2 = i2.lookup(k);
    assert!(v2.is_some());
    assert_eq!(v2.unwrap(), *v);
  }

  Ok(())
}
