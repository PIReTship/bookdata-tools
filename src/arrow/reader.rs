//! Support for streaming objects from a Parquet file.
use std::io::Seek;
use std::iter::zip;
use std::path::Path;
use std::thread::spawn;
use std::{fs::File, io::Read};

use anyhow::Result;
use crossbeam::channel::{bounded, Receiver, Sender};
use fallible_iterator::{FallibleIterator, IteratorExt};
use indicatif::ProgressBar;
use log::*;
use polars::prelude::*;
use polars_parquet::read::{infer_schema, read_metadata, FileReader};

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
    let mut reader = File::open(&path)?;
    let meta = read_metadata(&mut reader)?;
    let row_count = meta.num_rows;
    let schema = infer_schema(&meta)?;
    let reader = FileReader::new(reader, meta.row_groups, schema.clone(), None, None, None);

    info!(
        "scanning {} with {} rows",
        path.display(),
        friendly::scalar(row_count)
    );
    let pb = item_progress(row_count, &format!("{}", path.display()));

    // use a small bound since we're sending whole batches
    let (send, receive) = bounded(5);
    let p2 = path.to_path_buf();

    spawn(move || {
        let send = send;
        if let Err(e) = scan_backend(reader, &send, schema, &p2, pb) {
            send.send(Err(e)).expect("failed to send error message");
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

fn scan_backend<R: Read + Seek, E: TableRow + Send + Sync + 'static>(
    reader: FileReader<R>,
    send: &Sender<Result<Vec<E>>>,
    schema: ArrowSchema,
    path: &Path,
    pb: ProgressBar,
) -> Result<()> {
    debug!("{}: beginning batched read", path.display());
    let meter = meter_bar(5, &format!("{} read buffer", path.display()));
    for chunk in reader {
        let chunk = chunk?;

        trace!("{}: received batch of {} rows", path.display(), chunk.len());
        pb.inc(chunk.len() as u64);
        let columns: Vec<_> = zip(&schema.fields, chunk.into_arrays())
            .map(|(f, c)| Series::from_arrow(&f.name, c))
            .transpose_into_fallible()
            .collect()?;
        let df = DataFrame::new(columns)?;
        let batch = decode_chunk(df)?;
        trace!("{}: decoded chunk, sending to consumer", path.display());
        measure_and_send(&send, Ok(batch), &meter)?;
    }
    Ok(())
}
