//! Extract author information for book clusters.
use std::collections::HashMap;
use std::path::PathBuf;

use hex;
use md5::{Digest, Md5};

use crate::arrow::*;
use crate::prelude::*;
use polars::prelude::*;

use crate::prelude::Result;

#[derive(Args, Debug)]
#[command(name = "hash")]
/// Compute a hash for each cluster.
pub struct HashCmd {
    /// Specify output file
    #[arg(short = 'o', long = "output", name = "FILE")]
    output: PathBuf,

    /// Specify input file
    #[arg(name = "ISBN_CLUSTERS")]
    cluster_file: PathBuf,
}

#[derive(TableRow)]
struct ClusterHash {
    cluster: i32,
    isbn_hash: String,
    isbn_dcode: i8,
}

/// Load ISBN data
fn scan_isbns(path: &Path) -> Result<LazyFrame> {
    let path = path
        .to_str()
        .map(|s| s.to_string())
        .ok_or(anyhow!("invalid UTF8 pathname"))?;
    info!("scanning ISBN cluster file {}", path);
    let icl = LazyFrame::scan_parquet(path, default())?;
    let icl = icl.select(&[col("isbn"), col("cluster")]);
    Ok(icl)
}

impl Command for HashCmd {
    fn exec(&self) -> Result<()> {
        let isbns = scan_isbns(self.cluster_file.as_path())?;

        // It would be nice to do this with group-by, but group-by is quite slow and introduces
        // unhelpful overhead. Sorting (for consistency) and a custom loop to aggregate ISBNs
        // into hashes is much more efficient.
        info!("reading sorted ISBNs into memory");
        let isbns = isbns.sort("isbn", SortOptions::default()).collect()?;

        info!("computing ISBN hashes");
        let mut hashes: HashMap<i32, Md5> = HashMap::new();
        let isbn_col = isbns.column("isbn")?.str()?;
        let clus_col = isbns.column("cluster")?.i32()?;
        for pair in isbn_col.into_iter().zip(clus_col.into_iter()) {
            if let (Some(i), Some(c)) = pair {
                hashes.entry(c).or_default().update(i.as_bytes());
            }
        }

        info!("computed hashes for {} clusters", hashes.len());

        let path = self.output.as_path();
        info!("writing ISBN hashes to {:?}", path);
        let mut writer = TableWriter::open(path)?;
        for (cluster, h) in hashes.into_iter() {
            let h = h.finalize();
            writer.write_object(ClusterHash {
                cluster,
                isbn_hash: hex::encode(h),
                isbn_dcode: (h[h.len() - 1] % 2) as i8,
            })?;
        }

        writer.finish()?;

        Ok(())
    }
}
