//! Extract author information for book clusters.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use structopt::StructOpt;
use futures::{StreamExt};
use md5::{Md5, Digest};
use hex;

use serde::{Serialize, Deserialize};

use tokio;

use datafusion::prelude::*;
use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::arrow::row_de::RecordBatchDeserializer;

#[derive(StructOpt, Debug)]
#[structopt(name="cluster-authors")]
/// Extract cluster author data from extracted book data.
struct ClusterAuthors {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Specify output file
  #[structopt(short="o", long="output")]
  output: PathBuf,
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, TableRow, Default)]
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
async fn scan_isbns(ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
  let icl = ctx.read_parquet("isbn-clusters.parquet")?;
  let icl = icl.select_columns(&["isbn", "cluster"])?;
  let icl = icl.sort(vec![
    col("cluster").sort(true, true),
    col("isbn").sort(true, true)
  ])?;
  Ok(icl)
}

/// Write out cluster hah records to file, without duplicates.
async fn write_hashes_dedup<P: AsRef<Path>>(df: Arc<dyn DataFrame>, path: P) -> Result<()> {
  let path: &Path = path.as_ref();
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

#[tokio::main]
async fn main() -> Result<()> {
  let opts = ClusterAuthors::from_args();
  opts.common.init()?;

  let mut ctx = ExecutionContext::new();
  let df = scan_isbns(&mut ctx).await?;
  write_hashes_dedup(df, opts.output).await?;

  Ok(())
}
