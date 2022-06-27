//! Support for streaming objects from a Parquet file.
use std::path::Path;
use std::fs::File;
use std::sync::Arc;
use std::thread::spawn;
use std::mem::drop;

use arrow2::chunk::Chunk;
use log::*;
use anyhow::Result;
use crossbeam_channel::{Receiver, bounded};

use arrow2::array::{Array, StructArray};
use arrow2::io::parquet::read::FileReader;

use arrow2_convert::deserialize::*;

/// Iterator over deserialized records from a Parquet file.
pub struct RecordIter<R> where R: ArrowDeserialize + Send + Sync + 'static, for <'a> &'a R::ArrayType: IntoIterator {
  remaining: usize,
  channel: Receiver<Result<Vec<R>>>,
  batch: Option<std::vec::IntoIter<R>>,
}

impl <R> RecordIter<R> where R: ArrowDeserialize + Send + Sync + 'static, for <'a> &'a R::ArrayType: IntoIterator {
  pub fn remaining(&self) -> usize {
    self.remaining
  }
}

fn decode_chunk<R, E>(chunk: Result<Chunk<Arc<dyn Array>>, E>) -> Result<Vec<R>>
where
  R: ArrowDeserialize<Type=R> + Send + Sync + 'static,
  for <'a> &'a R::ArrayType: IntoIterator<Item=Option<R>>,
  E: std::error::Error + Send + Sync + 'static
{
  let chunk = chunk?;
  let sa = StructArray::try_new(R::data_type(), chunk.into_arrays(), None)?;
  let sa: Box<dyn Array> = Box::new(sa);
  let recs = sa.try_into_collection()?;
  Ok(recs)
}

/// Scan a Parquet file in a background thread and deserialize records.
pub fn scan_parquet_file<R, P>(path: P) -> Result<RecordIter<R>>
where
  P: AsRef<Path>,
  R: ArrowDeserialize<Type=R> + Send + Sync + 'static,
  for <'a> &'a R::ArrayType: IntoIterator<Item=Option<R>>
{
  let path = path.as_ref();
  let reader = File::open(path)?;
  let reader = FileReader::try_new(reader, None, None, None, None)?;
  let meta = reader.metadata();
  let row_count = meta.num_rows;
  info!("scanning {:?} with {} rows", path, row_count);
  drop(meta);

  // use a small bound since we're sending whole batches
  let (send, receive) = bounded(5);

  spawn(move || {
    let send = send;
    let reader = reader;

    for chunk in reader {
      let recs = decode_chunk(chunk);
      let recs = match recs {
        Ok(v) => v,
        Err(e) => {
          send.send(Err(e)).expect("channel send error");
          panic!("decode error in writer thread");
        }
      };
      send.send(Ok(recs)).expect("channel send error");
    }
  });

  Ok(RecordIter {
    remaining: row_count,
    channel: receive,
    batch: None,
  })
}

impl <R> Iterator for RecordIter<R>
where
  R: ArrowDeserialize + Send + Sync + 'static,
  for <'a> &'a R::ArrayType: IntoIterator
{
  type Item = Result<R>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      // try to get another item from the current batch
      let next = self.batch.as_mut().map(|i| i.next()).flatten();
      if let Some(row) = next {
        // we got something! return it
        self.remaining -= 1;
        return Some(Ok(row));
      } else if let Ok(br) = self.channel.recv() {
        // fetch a new batch and try again
        match br {
          Ok(batch) => {
            self.batch = Some(batch.into_iter());
          },
          Err(e) => {
            // error on the channel
            return Some(Err(e))
          }
        }
      } else {
        // we're done
        return None
      }
    }
  }
}
