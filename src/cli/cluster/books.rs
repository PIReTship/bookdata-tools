//! Extract author information for book clusters.
use std::path::{PathBuf};

use structopt::StructOpt;

use polars::prelude::*;
use crate::arrow::polars::nonnull_schema;
use crate::arrow::writer::open_parquet_writer;
use crate::prelude::*;
use crate::ids::codes::*;

static GRAPH_NODE_FILE: &str = "book-links/cluster-graph-nodes.parquet";

#[derive(StructOpt, Debug)]
#[structopt(name="extract-books")]
/// Extract cluster book codes for a particular namespace.
pub struct ExtractBooks {
  /// Specify output file
  #[structopt(short="o", long="output", parse(from_os_str))]
  output: PathBuf,

  /// Output numspaced book codes instead of original IDs.
  #[structopt(short="C", long="numspaced-book-codes")]
  book_codes: bool,

  /// Specify the name of the book code field.
  #[structopt(short="n", long="name", name="FIELD", default_value="book_code")]
  field_name: String,

  /// Specify an additional file to join into the results.
  #[structopt(long="join-file", name="LINKFILE", parse(from_os_str))]
  join_file: Option<PathBuf>,

  /// Speficy a field to read from the join file.
  #[structopt(long="join-field", name="LINKFIELD")]
  join_field: Option<String>,

  /// Extract book codes in namespace NS.
  #[structopt(name="NS")]
  namespace: String,
}

impl Command for ExtractBooks {
  fn exec(&self) -> Result<()> {
    let ns = NS::by_name(&self.namespace).ok_or(anyhow!("invalid namespace"))?;
    let data = LazyFrame::scan_parquet(GRAPH_NODE_FILE, default())?;

    let bc_col = if self.book_codes {
      info!("writing numspaced book codes in column {}", &self.field_name);
      col("book_code").alias(&self.field_name)
    } else {
      info!("writing source book IDs in column {}", &self.field_name);
      (col("book_code") - lit(ns.base())).alias(&self.field_name)
    };

    let filtered = data.filter(
      (col("book_code") / lit(NS_MULT_BASE)).eq(lit(ns.code()))
    ).select(&[
      bc_col, col("cluster")
    ]);

    let results = if let Some(jf) = &self.join_file {
      let join = LazyFrame::scan_parquet(jf, default())?;
      let join = filtered.join(join, &[col(&self.field_name)], &[col(&self.field_name)], JoinType::Left);
      if let Some(fld) = &self.join_field {
        join.select(&[col(&self.field_name), col(fld), col("cluster")])
      } else {
        join
      }
    } else {
      filtered
    };

    info!("collecting results");
    let mut frame = results.collect()?;
    frame.rechunk();
    info!("got {} book links", frame.height());
    let schema = nonnull_schema(&frame);
    let writer = open_parquet_writer(&self.output, schema)?;
    writer.write_and_finish(frame.iter_chunks())?;

    Ok(())
  }
}
