//! Support for streaming objects from a Parquet file.
use std::path::Path;
use std::fs::File;
use std::sync::Arc;
use std::thread::spawn;
use std::mem::drop;
use std::sync::mpsc::{Receiver, sync_channel};

use arrow2::chunk::Chunk;
use log::*;
use anyhow::Result;

use arrow2::array::{Array, StructArray};
use arrow2::io::parquet::read::FileReader;

use arrow2_convert::deserialize::*;

/// Iterator over deserialized records from a Parquet file.
pub struct RecordIter<R> where R: ArrowDeserialize + Send + Sync + 'static, for <'a> &'a R::ArrayType: IntoIterator {
  remaining: usize,
  channel: Receiver<Result<R>>,
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

  let (send, receive) = sync_channel(500);

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
      for rec in recs {
        send.send(Ok(rec)).expect("channel send error");
      }
    }
  });

  Ok(RecordIter {
    remaining: row_count,
    channel: receive,
  })
}

impl <R> Iterator for RecordIter<R>
where
  R: ArrowDeserialize + Send + Sync + 'static,
  for <'a> &'a R::ArrayType: IntoIterator
{
  type Item = Result<R>;

  fn next(&mut self) -> Option<Self::Item> {
    if let Ok(res) = self.channel.recv() {
      self.remaining -= 1;
      Some(res)
    } else {
      None
    }
  }
}
