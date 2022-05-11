//! Support for collecting keys and counting the records that contributed them.
use std::sync::Arc;
use std::fs::File;
use std::path::Path;

#[cfg(test)]
use std::mem::drop;

use arrow::{
  array::*,
  record_batch::RecordBatch,
  datatypes::*
};
use parquet::{
  arrow::ArrowWriter,
  basic::Compression,
  file::properties::WriterProperties,
};
use super::index::IdIndex;

use anyhow::Result;

/// A key collector accumulates keys and associates them with numeric identifiers.
///
/// This structure is designed to count the number of records referencing each key,
/// divided by label (such as different sources).  It is optimized for use cases where
/// records are read label-by-label.
pub struct KeyCollector {
  index: IdIndex<String>,
  count_labels: Vec<String>,
  counts: Vec<Vec<i32>>,
}

pub struct KeyCountAccum<'a> {
  index: &'a mut IdIndex<String>,
  counts: &'a mut Vec<i32>,
}

impl KeyCollector {
  /// Create a new key collector.
  pub fn new() -> KeyCollector {
    KeyCollector {
      index: IdIndex::new(),
      count_labels: Vec::new(),
      counts: Vec::new(),
    }
  }

  pub fn len(&self) -> usize {
    self.index.len()
  }

  /// Create an accumulator to count keys with a particular source label.
  pub fn accum<'a>(&'a mut self, label: &str) -> KeyCountAccum<'a> {
    let mut index = None;
    for i in 0..self.count_labels.len() {
      if self.count_labels[i] == label {
        index = Some(i);
        break;
      }
    }
    let vec = if let Some(i) = index {
      &mut self.counts[i]
    } else {
      let i = self.count_labels.len();
      self.count_labels.push(label.to_string());
      let mut vec = Vec::with_capacity(self.index.len());
      vec.resize(self.len(), 0);
      self.counts.push(vec);
      &mut self.counts[i]
    };
    KeyCountAccum {
      index: &mut self.index,
      counts: vec
    }
  }

  /// Save to a Parquet file.
  pub fn save<P: AsRef<Path>>(&mut self, key_col: &str, id_col: &str, path: P) -> Result<usize> {
    let batch = self.to_record_batch(key_col, id_col)?;
    let file = File::create(path)?;
    let props = WriterProperties::builder();
    let props = props.set_dictionary_enabled(false);
    let props = props.set_compression(Compression::ZSTD);
    let props = props.build();
    let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props))?;

    w.write(&batch)?;
    let md = w.close()?;

    Ok(md.num_rows as usize)
  }

  /// Create an Arrow [RecordBatch] for this accumulator.
  pub fn to_record_batch(&mut self, key_col: &str, id_col: &str) -> Result<RecordBatch> {
    let len = self.len();
    let mut fields = vec![
      Field::new(id_col, DataType::Int32, false),
      Field::new(key_col, DataType::Utf8, false)
    ];
    for l in &self.count_labels {
      fields.push(Field::new(l, DataType::Int32, false))
    }
    let schema = Schema::new(fields);
    let schema = Arc::new(schema);

    let keys = self.index.key_vec();
    let mut id_arr = Int32Builder::new(self.index.len());
    let mut key_arr = StringBuilder::new(self.index.len());
    for i in 0..keys.len() {
      id_arr.append_value((i + 1) as i32)?;
      key_arr.append_value(keys[i])?;
    }
    let mut cols: Vec<ArrayRef> = vec![
      Arc::new(id_arr.finish()),
      Arc::new(key_arr.finish())
    ];

    for cts in &mut self.counts {
      cts.resize(len, 0);
      let c_arr = Int32Array::from_iter_values(cts.iter().map(|i| *i));
      cols.push(Arc::new(c_arr));
    }


    let batch = RecordBatch::try_new(schema, cols)?;

    Ok(batch)
  }
}

impl <'a> KeyCountAccum<'a> {
  /// Add a key to this accumulator.
  pub fn add_key(&mut self, key: &str) {
    let pos = self.index.intern(key).expect("intern failure") as usize - 1;
    if pos >= self.counts.len() {
      self.counts.resize(pos + 1, 0);
    }
    self.counts[pos] += 1;
  }

  /// Add an iterator's worth of keys to this accumulator.
  pub fn add_keys<I: Iterator>(&mut self, iter: I) where I::Item: AsRef<str> {
    for s in iter {
      let s = s.as_ref();
      self.add_key(s);
    }
  }
}


#[test]
fn test_empty() {
  let kc = KeyCollector::new();
  assert_eq!(kc.index.len(), 0);
  assert_eq!(kc.count_labels.len(), 0);
  assert_eq!(kc.counts.len(), 0);
}


#[test]
fn test_acc_1() {
  let mut kc = KeyCollector::new();
  let mut acc = kc.accum("bob");
  acc.add_key("wumpus");
  assert_eq!(kc.index.len(), 1);
  assert_eq!(kc.count_labels.len(), 1);
  assert_eq!(kc.counts.len(), 1);
  assert!(kc.index.lookup("wumpus").is_some());
  assert_eq!(kc.count_labels[0], "bob");
  assert_eq!(kc.counts[0][0], 1);
}


#[test]
fn test_acc_again() {
  let mut kc = KeyCollector::new();
  let mut acc = kc.accum("bob");
  acc.add_key("wumpus");
  drop(acc);
  let mut ac2 = kc.accum("albert");
  ac2.add_key("zzzz");
  drop(ac2);
  assert_eq!(kc.index.len(), 2);
  assert_eq!(kc.count_labels.len(), 2);
  assert_eq!(kc.counts.len(), 2);
  assert!(kc.index.lookup("wumpus").is_some());
  assert!(kc.index.lookup("zzzz").is_some());
  assert_eq!(kc.count_labels[0], "bob");
  assert_eq!(kc.counts[0][0], 1);
  assert_eq!(kc.count_labels[1], "albert");
  assert_eq!(kc.counts[1][1], 1);
}
