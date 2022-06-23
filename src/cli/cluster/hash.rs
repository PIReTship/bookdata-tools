//! Extract author information for book clusters.
use std::path::{PathBuf};
use std::fs::File;
use std::sync::Arc;

use structopt::StructOpt;
use md5::{Md5, Digest};
use hex;

use polars::prelude::*;
use crate::prelude::*;

use crate::prelude::Result;

#[derive(StructOpt, Debug)]
#[structopt(name="hash")]
/// Compute a hash for each cluster.
pub struct HashCmd {
  /// Specify output file
  #[structopt(short="o", long="output")]
  output: PathBuf,
}

fn hash_isbns(isbns: Series) -> Result<Series, PolarsError> {
  let s2 = isbns.sort(false);
  let s2 = s2.utf8()?;
  let mut hash = Md5::default();
  for elt in s2 {
    if let Some(isbn) = elt {
      hash.update(isbn.as_bytes());
    }
  }
  let res = Series::new("isbn_hash", vec![hex::encode(hash.finalize())]);
  Ok(res)
}

fn hash_field(_schema: &Schema, _ctx: Context, _fields: &[Field]) -> Field {
  Field::new("isbn_hash", DataType::Utf8)
}

/// Load ISBN data
fn scan_isbns() -> Result<LazyFrame> {
  let icl = LazyFrame::scan_parquet("isbn-clusters.parquet".to_string(), default())?;
  let icl = icl.select(&[col("isbn"), col("cluster")]);
  Ok(icl)
}

impl Command for HashCmd {
  fn exec(&self) -> Result<()> {
    let isbns = scan_isbns()?;
    let hashes = isbns.groupby(&[col("cluster")]).agg(&[
      col("isbn").apply(hash_isbns, NoEq::new(Arc::new(hash_field)))
    ]);

    info!("computing ISBN hashes");
    let mut hashes = hashes.collect()?;
    let (nr, nc) = hashes.shape();
    assert_eq!(nc, 2);
    info!("computed hashes for {} clusters", nr);

    let path = self.output.as_path();
    info!("saving Parquet to {:?}", path);
    let file = File::create(path)?;
    let write = ParquetWriter::new(file).with_compression(ParquetCompression::Zstd(None));
    write.finish(&mut hashes)?;

    Ok(())
  }
}
