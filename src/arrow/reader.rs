//! Support for streaming objects from a Parquet file.
use std::fs::File;
use std::path::Path;
use std::thread::spawn;

use anyhow::Result;
use crossbeam::channel::{bounded, Receiver};
use log::*;
use polars::{io::pl_async::get_runtime, prelude::*};

use crate::{
    arrow::row::iter_df_rows,
    util::logging::{item_progress, measure_and_send, meter_bar},
};

use super::TableRow;

/// Scan a Parquet file into a data frame.
pub fn scan_df_parquet<P: AsRef<Path>>(file: P) -> Result<LazyFrame> {
    let file = file.as_ref();
    debug!("scanning file {}", file.display());
    let df = LazyFrame::scan_parquet(file, ScanArgsParquet::default())?;
    debug!("{}: schema {:?}", file.display(), df.schema()?);
    Ok(df)
}

/// Iterator over deserialized records from a Parquet file.
pub struct RecordIter<R>
where
    R: TableRow + Send + Sync + 'static,
{
    remaining: usize,
    channel: Receiver<Result<Vec<R>>>,
    batch: Option<std::vec::IntoIter<R>>,
}

impl<R> RecordIter<R>
where
    R: TableRow + Send + Sync + 'static,
{
    pub fn remaining(&self) -> usize {
        self.remaining
    }
}

fn decode_chunk<R>(chunk: DataFrame) -> Result<Vec<R>>
where
    R: TableRow + Send + Sync + 'static,
{
    let iter = iter_df_rows(&chunk)?;
    let mut records = Vec::with_capacity(chunk.height());
    for row in iter {
        records.push(row?);
    }
    Ok(records)
}

/// Scan a Parquet file in a background thread and deserialize records.
pub fn scan_parquet_file<R, P>(path: P) -> Result<RecordIter<R>>
where
    P: AsRef<Path>,
    R: TableRow + Send + Sync + 'static,
{
    let path = path.as_ref();
    let reader = File::open(&path)?;
    let mut reader = ParquetReader::new(reader)
        .set_low_memory(true)
        .read_parallel(ParallelStrategy::None);
    let row_count = reader.num_rows()?;

    info!(
        "scanning {} with {} rows",
        path.display(),
        friendly::scalar(row_count)
    );
    debug!("file schema: {:?}", reader.get_metadata()?.schema());
    let pb = item_progress(row_count, &format!("{}:", path.display()));

    let reader = reader.batched(1024 * 1024)?;

    // use a small bound since we're sending whole batches
    let (send, receive) = bounded(5);
    let fill = meter_bar(5, &format!("{} scan buffer", path.display()));
    let p2 = path.to_path_buf();

    spawn(move || {
        let send = send;
        let mut reader = reader;

        debug!("{}: beginning batched read", p2.display());
        while !reader.is_finished() {
            trace!("{}: fetching batch", p2.display());
            let batches = reader.next_batches(1);
            let batches = get_runtime().block_on_potential_spawn(batches);
            let batches = match batches {
                Ok(bs) => bs,
                Err(e) => {
                    debug!("routing backend error {:?}", e);
                    send.send(Err(e.into())).expect("channel send error");
                    return;
                }
            };

            for df in batches.into_iter().flatten() {
                trace!("{}: received batch of {} rows", p2.display(), df.height());
                pb.inc(df.height() as u64);
                match decode_chunk(df) {
                    Ok(batch) => {
                        measure_and_send(&send, Ok(batch), &fill).expect("channel send error");
                    }
                    Err(e) => {
                        debug!("routing backend error {:?}", e);
                        send.send(Err(e.into())).expect("channel send error");
                        return;
                    }
                }
            }
        }
    });

    debug!(
        "{}: background thread spawned, returning iter",
        path.display()
    );
    Ok(RecordIter {
        remaining: row_count,
        channel: receive,
        batch: None,
    })
}

impl<R> Iterator for RecordIter<R>
where
    R: TableRow + Send + Sync + 'static,
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
                    }
                    Err(e) => {
                        // error on the channel
                        return Some(Err(e));
                    }
                }
            } else {
                // we're done
                return None;
            }
        }
    }
}
