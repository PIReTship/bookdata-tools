//! Support for collecting keys and counting the records that contributed them.
use std::fs::File;
use std::path::Path;

#[cfg(test)]
use std::mem::drop;

use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::write::{FileWriter, WriteOptions};
use log::*;
use parquet2::write::Version;
use polars::prelude::*;

use super::index::IdIndex;
use crate::io::ObjectWriter;
use crate::util::logging::data_progress;

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
            counts: vec,
        }
    }

    /// Save to a Parquet file.
    pub fn save<P: AsRef<Path>>(&mut self, key_col: &str, id_col: &str, path: P) -> Result<usize> {
        info!("saving accumulated keys to {:?}", path.as_ref());

        debug!("creating data frame");
        let df = self.to_data_frame(id_col, key_col)?;

        debug!("opening file");
        let file = File::create(path)?;
        let options = WriteOptions {
            write_statistics: true,
            version: Version::V2,
            compression: parquet2::compression::CompressionOptions::Zstd(None),
            data_pagesize_limit: None,
        };
        let mut writer = FileWriter::try_new(file, self.schema(id_col, key_col), options)?;

        let pb = data_progress(df.n_chunks());
        for chunk in df.iter_chunks() {
            writer.write_object(chunk)?;
            pb.tick();
        }

        // FIXME use write_object
        writer.end(None)?;

        Ok(df.height())
    }

    /// Get an Arrow [Schema] for this key collector.
    pub fn schema(&self, id_col: &str, key_col: &str) -> Schema {
        let mut fields = vec![
            Field::new(id_col, DataType::Int32, false),
            Field::new(key_col, DataType::Utf8, false),
        ];
        for l in &self.count_labels {
            fields.push(Field::new(l, DataType::Int32, false))
        }
        Schema {
            fields,
            metadata: Default::default(),
        }
    }

    /// Create an Polars [DataFrame] for this accumulator's data.
    pub fn to_data_frame(&mut self, id_col: &str, key_col: &str) -> Result<DataFrame> {
        let len = self.len();
        let mut df = self.index.data_frame(id_col, key_col)?;

        for i in 0..self.counts.len() {
            let name = self.count_labels[i].as_str();
            let counts = &mut self.counts[i];
            counts.resize(len, 0);
            let col = Int32Chunked::new(name, counts);
            df.with_column(col)?;
        }

        Ok(df)
    }
}

impl<'a> KeyCountAccum<'a> {
    /// Add a key to this accumulator.
    pub fn add_key(&mut self, key: &str) {
        let pos = self.index.intern(key).expect("intern failure") as usize - 1;
        if pos >= self.counts.len() {
            self.counts.resize(pos + 1, 0);
        }
        self.counts[pos] += 1;
    }

    /// Add an iterator's worth of keys to this accumulator.
    pub fn add_keys<I: Iterator>(&mut self, iter: I)
    where
        I::Item: AsRef<str>,
    {
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
