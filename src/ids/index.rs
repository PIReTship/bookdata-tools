//! Data structure for mapping string keys to numeric identifiers.
use std::path::{Path};
use std::fs::File;
use std::sync::Arc;
use std::hash::Hash;
use std::borrow::Borrow;
#[cfg(test)]
use arrow2::io::parquet::read::read_metadata;
use hashbrown::hash_map::{HashMap, Keys};

use serde::de::DeserializeOwned;
use log::*;
use anyhow::Result;
use thiserror::Error;
use polars::prelude::*;
use parquet::record::reader::RowIter;
use parquet::record::RowAccessor;
use parquet::basic::{Type as PhysicalType, LogicalType, Repetition};
use parquet::schema::types::Type;
use crate::arrow::*;

#[cfg(test)]
use quickcheck::{Arbitrary,Gen};
#[cfg(test)]
use tempfile::tempdir;

/// The type of index identifiers.
pub type Id = i32;

#[derive(Error, Debug)]
pub enum IndexError {
  #[error("key not present in frozen index")]
  KeyNotPresent
}

/// Index identifiers from a data type
pub struct IdIndex<K> {
  map: HashMap<K,Id>,
  frozen: bool,
}

/// Internal struct for ID records.
#[derive(ParquetRecordWriter)]
struct IdRec {
  id: i32,
  key: String,
}

impl <K> IdIndex<K> where K: Eq + Hash {
  /// Create a new index.
  pub fn new() -> IdIndex<K> {
    IdIndex {
      map: HashMap::new(),
      frozen: false,
    }
  }

  /// Freeze the index so no new items can be added.
  #[allow(dead_code)]
  pub fn freeze(self) -> IdIndex<K> {
    IdIndex { map: self.map, frozen: true }
  }

  /// Get the index length
  pub fn len(&self) -> usize {
    self.map.len()
  }

  /// Get the ID for a key, adding it to the index if needed.
  pub fn intern<Q>(&mut self, key: &Q) -> Result<Id, IndexError> where K: Borrow<Q>, Q: Hash + Eq + ToOwned<Owned=K> + ?Sized {
    let n = self.map.len() as Id;
    if self.frozen {
      self.lookup(key).ok_or(IndexError::KeyNotPresent)
    } else {
      // use Hashbrown's raw-entry API to minimize cloning
      let eb = self.map.raw_entry_mut();
      let e = eb.from_key(key);
      let (_, v) = e.or_insert_with(|| {
        (key.to_owned(), n+1)
      });
      Ok(*v)
    }
  }

  /// Get the ID for a key, adding it to the index if needed and transferring ownership.
  pub fn intern_owned(&mut self, key: K) -> Result<Id, IndexError> {
    let n = self.map.len() as Id;
    if self.frozen {
      self.lookup(&key).ok_or(IndexError::KeyNotPresent)
    } else {
      Ok(*self.map.entry(key).or_insert(n+1))
    }
  }

  /// Look up the ID for a key if it is present.
  #[allow(dead_code)]
  pub fn lookup<Q>(&self, key: &Q) -> Option<Id> where K: Borrow<Q>, Q: Hash + Eq + ?Sized {
    self.map.get(key).map(|i| *i)
  }

  /// Iterate over keys (see [std::collections::HashMap::keys]).
  #[allow(dead_code)]
  pub fn keys(&self) -> Keys<'_, K, Id> {
    self.map.keys()
  }

  /// Get the keys in order.
  pub fn key_vec(&self) -> Vec<&K> {
    let mut vec = Vec::with_capacity(self.len());
    vec.resize(self.len(), None);
    for (k, n) in self.map.iter() {
      let i = (n - 1) as usize;
      assert!(vec[i].is_none());
      vec[i] = Some(k);
    }

    let vec = vec.iter().map(|ro| ro.unwrap()).collect();
    vec
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
    let path_str = path.as_ref().to_string_lossy();
    info!("reading index from file {}", path_str);
    let read = open_parquet_file(path.as_ref())?;

    let file_schema = read.metadata().file_metadata().schema();
    debug!("file schema: {:?}", file_schema);

    let id_type = LogicalType::Integer { bit_width: 32, is_signed: true };
    let key_type = LogicalType::String;
    let mut tgt_fields = vec![
      Arc::new(Type::primitive_type_builder(id_col, PhysicalType::INT32)
        .with_logical_type(Some(id_type))
        .with_repetition(Repetition::OPTIONAL)
        .build()?),
      Arc::new(Type::primitive_type_builder(key_col, PhysicalType::BYTE_ARRAY)
        .with_logical_type(Some(key_type))
        .with_repetition(Repetition::OPTIONAL)
        .build()?),
    ];
    let tgt_schema = Type::group_type_builder(file_schema.name()).with_fields(&mut tgt_fields).build()?;
    debug!("target schema: {:?}", tgt_schema);

    let read = RowIter::from_file(Some(tgt_schema), &read)?;
    let mut map = HashMap::new();

    debug!("reading file contents");
    for row in read {
      let id = row.get_int(0)?;
      let key = row.get_string(1)?;
      map.insert(key.clone(), id);
    }

    info!("read {} keys from {}", map.len(), path_str);

    Ok(IdIndex {
      map, frozen: false
    })
  }

  /// Load an index from a CSV file.
  ///
  /// This loads an index from a CSV file.  It assumes the first column is the ID, and the
  /// second column is the key.
  #[allow(dead_code)]
  pub fn load_csv<P: AsRef<Path>, K: Eq + Hash + DeserializeOwned>(path: P) -> Result<IdIndex<K>> {
    info!("reading ID index from from {:?}", path.as_ref());
    let input = csv::Reader::from_path(path)?;
    let recs = input.into_deserialize();

    let mut map = HashMap::new();

    for row in recs {
      let rec: (i32, K) = row?;
      let (id, key) = rec;
      map.insert(key, id);
    }

    Ok(IdIndex {
      map, frozen: false
    })
  }

  /// Save to a Parquet file with the standard configuration.
  pub fn save_standard<P: AsRef<Path>>(&self, path: P) -> Result<()> {
    self.save(path, "id", "key")
  }

  /// Save to a Parquet file with the standard configuration.
  pub fn save<P: AsRef<Path>>(&self, path: P, id_col: &str, key_col: &str) -> Result<()> {
    debug!("preparing data frame for index");
    let mut ids = Vec::with_capacity(self.map.len());
    let mut keys = Vec::with_capacity(self.map.len());

    for (k, v) in &self.map {
      ids.push(*v);
      keys.push(k.as_str());
    }

    let ids = Series::from_vec(id_col, ids);
    let keys = Series::new(key_col, &keys);
    let mut frame = DataFrame::new(vec![ids, keys])?;

    let path = path.as_ref();
    info!("saving index to {:?}", path);
    let file = File::create(path)?;
    let writer = ParquetWriter::new(file)
      .with_compression(ParquetCompression::Zstd(None));
    writer.finish(&mut frame)?;

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
  let id = index.intern("hackem muche").expect("intern failure");
  assert_eq!(id, 1);
  assert_eq!(index.lookup("hackem muche").unwrap(), 1);
}


#[test]
fn test_index_intern_two() {
  let mut index: IdIndex<String> = IdIndex::new();
  assert!(index.lookup("hackem muche").is_none());
  let id = index.intern("hackem muche");
  assert_eq!(id.expect("intern failure"), 1);
  let id2 = index.intern("readme");
  assert_eq!(id2.expect("intern failure"), 2);
  assert_eq!(index.lookup("hackem muche").unwrap(), 1);
}


#[test]
fn test_index_intern_twice() {
  let mut index: IdIndex<String> = IdIndex::new();
  assert!(index.lookup("hackem muche").is_none());
  let id = index.intern("hackem muche");
  assert_eq!(id.expect("intern failure"), 1);
  let id2 = index.intern("hackem muche");
  assert_eq!(id2.expect("intern failure"), 1);
  assert_eq!(index.len(), 1);
}

#[test]
fn test_index_intern_twice_owned() {
  let mut index: IdIndex<String> = IdIndex::new();
  assert!(index.lookup("hackem muche").is_none());
  let id = index.intern_owned("hackem muche".to_owned());
  assert!(id.is_ok());
  assert_eq!(id.expect("intern failure"), 1);
  let id2 = index.intern_owned("hackem muche".to_owned());
  assert!(id2.is_ok());
  assert_eq!(id2.expect("intern failure"), 1);
  assert_eq!(index.len(), 1);
}


#[cfg(test)]
#[test_log::test]
fn test_index_save() -> Result<()> {
  let mut index: IdIndex<String> = IdIndex::new();
  let mut gen = Gen::new(100);
  for _i in 0..10000 {
    let key = String::arbitrary(&mut gen);
    let prev = index.lookup(&key);
    let id = index.intern(&key).expect("intern failure");
    match prev {
      Some(i) => assert_eq!(id, i),
      None => assert_eq!(id as usize, index.len())
    };
  }

  let dir = tempdir()?;
  let pq = dir.path().join("index.parquet");
  index.save_standard(&pq).expect("save error");

  let mut pqf = File::open(&pq).expect("open error");
  let meta = read_metadata(&mut pqf).expect("meta error");
  println!("file metadata: {:?}", meta);
  std::mem::drop(pqf);

  let i2 = IdIndex::load_standard(&pq).expect("load error");
  assert_eq!(i2.len(), index.len());
  for (k, v) in &index.map {
    let v2 = i2.lookup(k);
    assert!(v2.is_some());
    assert_eq!(v2.unwrap(), *v);
  }

  Ok(())
}


#[test]
fn test_index_freeze() {
  let mut index: IdIndex<String> = IdIndex::new();
  assert!(index.lookup("hackem muche").is_none());
  let id = index.intern("hackem muche");
  assert!(id.is_ok());
  assert_eq!(id.expect("intern failure"), 1);

  let mut index = index.freeze();

  let id = index.intern("hackem muche");
  assert!(id.is_ok());
  assert_eq!(id.expect("intern failure"), 1);

  let id2 = index.intern("foobie bletch");
  assert!(id2.is_err());
}
