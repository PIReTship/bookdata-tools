use std::str::FromStr;
use std::sync::Arc;
use std::path::PathBuf;

use serde::{Deserialize};
use serde_json::from_str;
use arrow::datatypes::*;
use arrow::array::*;
use paste::paste;

use bookdata::prelude::*;
use bookdata::io::LineProcessor;
use bookdata::parquet::*;
use bookdata::index::IdIndex;

/// Scan GoodReads interaction file into Parquet
#[derive(StructOpt)]
#[structopt(name="scan-interactions")]
pub struct ScanInteractions {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf,

  /// Ouptut file
  #[structopt(name = "OUTPUT", parse(from_os_str))]
  outfile: PathBuf
}

// the records we read from JSON
#[derive(Deserialize)]
struct RawInteraction {
  user_id: String,
  book_id: String,
  // review_id: String,
  #[serde(rename="isRead")]
  is_read: bool,
  rating: i32,
  // date_added: String
}

impl FromStr for RawInteraction {
  type Err = serde_json::Error;

  fn from_str(s: &str) -> serde_json::Result<RawInteraction> {
    return from_str(s)
  }
}

macro_rules! table_record {
  (struct $rn:ident { $($fn:ident : $ft:ty),* }) => {
    struct $rn {
      $($fn: $ft),*
    }

    paste! {
      struct [<$rn Batch>] {
        $($fn: <$ft as ArrowTypeInfo>::PQArrayBuilder),*
      }
    }

    impl TableRow for $rn {
      paste! {
        type Batch = [<$rn Batch>];
      }

      fn schema() -> Schema {
        Schema::new(vec![
          $(<$ft as ArrowTypeInfo>::field(stringify!($fn))),*
        ])
      }

      fn new_batch(cap: usize) -> Self::Batch {
        Self::Batch {
          $($fn: <$ft as ArrowTypeInfo>::PQArrayBuilder::new(cap)),*
        }
      }

      fn finish_batch(batch: &mut Self::Batch) -> Vec<ArrayRef> {
        vec![
          $(Arc::new(batch.$fn.finish())),*
        ]
      }

      fn write_to_batch(&self, batch: &mut Self::Batch) -> Result<()> {
        $(
          self.$fn.append_to_builder(&mut batch.$fn)?;
        )*
        Ok(())
      }
    }
  };
}

// the records we're actually going to write to the table
table_record!{
  struct IntRecord {
    rec_id: u64,
    user_id: u64,
    book_id: u64,
    is_read: u8,
    rating: Option<i8>
  }
}

fn main() -> Result<()> {
  let options = ScanInteractions::from_args();
  options.common.init()?;

  let infn = &options.infile;
  let outfn = &options.outfile;
  info!("reading interactions from {:?}", infn);
  let proc = LineProcessor::open_gzip(infn)?;
  let mut users = IdIndex::new();

  info!("writing interactions to {:?}", outfn);
  let mut writer = TableWriter::open(outfn)?;
  let mut n_recs = 0;

  for rec in proc.records() {
    let row: RawInteraction = rec?;
    let rec_id = n_recs + 1;
    n_recs += 1;
    let key = hex::decode(row.user_id.as_bytes())?;
    let user_id = users.intern(key);
    let book_id: u64 = row.book_id.parse()?;
    let record = IntRecord {
      rec_id, user_id, book_id,
      is_read: row.is_read as u8,
      rating: if row.rating > 0 {
        Some(row.rating as i8)
      } else {
        None
      }
    };
    writer.write(&record)?;
  }

  let nlines = writer.finish()?;

  info!("wrote {} records for {} users", nlines, users.len());

  Ok(())
}
