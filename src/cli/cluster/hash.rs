//! Extract author information for book clusters.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use structopt::StructOpt;
use futures::{StreamExt};
use md5::{Md5, Digest};
use hex;

use serde::{Serialize, Deserialize};

use datafusion::prelude::*;
use crate::prelude::*;
use crate::arrow::*;
use crate::arrow::row_de::RecordBatchDeserializer;
use crate::cli::AsyncCommand;
use async_trait::async_trait;

#[derive(StructOpt, Debug)]
#[structopt(name="hash")]
/// Compute a hash for each cluster.
pub struct HashCmd {
  /// Specify output file
  #[structopt(short="o", long="output")]
  output: PathBuf,
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, ParquetRecordWriter, Default)]
struct ClusterHash {
  cluster: i32,
  isbn_hash: String,
  isbn_dcode: i8,
}

#[derive(Deserialize, Clone, Default)]
struct ClusterIsbn {
  cluster: i32,
  isbn: String,
}

/// Load ISBN data
async fn scan_isbns(ctx: &mut SessionContext) -> Result<Arc<DataFrame>> {
  let icl = ctx.read_parquet("isbn-clusters.parquet", default()).await?;
  let icl = icl.select_columns(&["isbn", "cluster"])?;
  let icl = icl.sort(vec![
    col("cluster").sort(true, true),
    col("isbn").sort(true, true)
  ])?;
  Ok(icl)
}

/// Write out cluster hah records to file, without duplicates.
async fn write_hashes_dedup(df: Arc<DataFrame>, path: &Path) -> Result<()> {
  info!("saving output to {}", path.to_string_lossy());
  let mut writer = TableWriter::open(path)?;

  info!("scanning author batches");
  let stream = df.execute_stream().await?;
  let mut cluster = -1;
  let mut hash = Md5::default();
  let mut rec_stream = RecordBatchDeserializer::for_stream(stream);
  while let Some(row) = rec_stream.next().await {
    let row: ClusterIsbn = row?;
    if row.cluster != cluster {
      if cluster >= 0 {
        let h = hash.finalize_reset();
        writer.write_object(ClusterHash {
          cluster,
          isbn_hash: hex::encode(h),
          isbn_dcode: (h[h.len() - 1] % 2) as i8
        })?;
      }

      cluster = row.cluster;
    }

    hash.update(row.isbn.as_bytes());
  }

  if cluster >= 0 {
    let h = hash.finalize_reset();
    writer.write_object(ClusterHash {
      cluster,
      isbn_hash: hex::encode(h),
      isbn_dcode: (h[h.len() - 1] % 2) as i8
    })?;
  }

  let n = writer.finish()?;
  info!("wrote {} cluster-author links", n);

  Ok(())
}

#[async_trait]
impl AsyncCommand for HashCmd {
  async fn exec_future(&self) -> Result<()> {
    let mut ctx = SessionContext::new();
    let df = scan_isbns(&mut ctx).await?;
    write_hashes_dedup(df, &self.output).await?;

    Ok(())
  }
}
