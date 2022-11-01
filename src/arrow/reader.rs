//! Support for streaming objects from a Parquet file.
use std::path::Path;
use std::fs::File;
use std::thread::spawn;
use std::sync::mpsc::{Receiver, sync_channel};

use arrow2::chunk::Chunk;
use log::*;
use anyhow::Result;

use arrow2::array::{Array, StructArray};
use arrow2::io::parquet::read::{FileReader, read_metadata, infer_schema};

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

fn decode_chunk<R, E>(chunk: Result<Chunk<Box<dyn Array>>, E>) -> Result<Vec<R>>
where
  R: ArrowDeserialize<Type=R> + Send + Sync + 'static,
  for <'a> &'a R::ArrayType: IntoIterator<Item=Option<R>>,
  E: std::error::Error + Send + Sync + 'static
{
  let chunk = chunk?;
  let chunk_size = chunk.len();
  let sa = StructArray::try_new(R::data_type(), chunk.into_arrays(), None).map_err(|e| {
    error!("error decoding struct array: {:?}", e);
    e
  })?;
  let sa: Box<dyn Array> = Box::new(sa);
  let sadt = sa.data_type().clone();
  let recs: Vec<R> = sa.try_into_collection().map_err(|e| {
    error!("error deserializing batch: {:?}", e);
    info!("chunk schema: {:?}", sadt);
    info!("target schema: {:?}", R::data_type());
    e
  })?;
  assert_eq!(recs.len(), chunk_size);
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
  let mut reader = File::open(path)?;

  let meta = read_metadata(&mut reader)?;
  let schema = infer_schema(&meta)?;
  let row_count = meta.num_rows;
  let row_groups = meta.row_groups;

  let reader = FileReader::new(reader, row_groups, schema,None, None, None);
  info!("scanning {:?} with {} rows", path, row_count);
  debug!("file schema: {:?}", meta.schema_descr);

  // use a small bound since we're sending whole batches
  let (send, receive) = sync_channel(5);

  spawn(move || {
    let send = send;
    let reader = reader;

    for chunk in reader {
      let recs = decode_chunk(chunk);
      let recs = match recs {
        Ok(v) => v,
        Err(e) => {
          debug!("routing backend error {:?}", e);
          send.send(Err(e)).expect("channel send error");
          return;
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
            // loop around and use the batch
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
