//! Extract author information for book clusters.
use std::path::{PathBuf};
use std::fs::File;

use arrow2::io::parquet::write::{FileWriter, WriteOptions};
use parquet2::write::Version;
use structopt::StructOpt;
use parse_display::{Display, FromStr};

use polars::prelude::*;
use crate::io::object::ThreadObjectWriter;
use crate::prelude::*;
use crate::arrow::dfext::*;
use anyhow::Result;

#[derive(Display, FromStr, Debug)]
#[display(style="lowercase")]
enum Source {
  OpenLib,
  LOC,
}

#[derive(StructOpt, Debug)]
#[structopt(name="extract-authors")]
/// Extract cluster author data from extracted book data.
pub struct ClusterAuthors {
  /// Only extract first authors
  #[structopt(long="first-author")]
  first_author: bool,

  /// Specify output file
  #[structopt(short="o", long="output")]
  output: PathBuf,

  /// Specify the source
  #[structopt(short="s", long="source")]
  sources: Vec<Source>
}

// #[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, ArrowField, Default)]
// struct ClusterAuthor {
//   cluster: i32,
//   author_name: String
// }

// /// Write out author records to file, without duplicates.
// async fn write_authors_dedup<P: AsRef<Path>>(df: Arc<DataFrame>, path: P) -> Result<()> {
//   let mut writer = TableWriter::open(path)?;

//   info!("scanning author batches");
//   let stream = df.execute_stream().await?;
//   let mut last = ClusterAuthor::default();
//   let mut rec_stream = RecordBatchDeserializer::for_stream(stream);
//   while let Some(row) = rec_stream.next().await {
//     let row: ClusterAuthor = row?;
//     if row != last {
//       writer.write_object(row.clone())?;
//       last = row;
//     }
//   }

//   let n = writer.finish()?;
//   info!("wrote {} cluster-author links", n);

//   Ok(())
// }

/// Scan the OpenLibrary data for authors.
fn scan_openlib(first_only: bool) -> Result<LazyFrame> {
  info!("scanning OpenLibrary author data");
  info!("reading ISBN clusters");
  let icl = LazyFrame::scan_parquet("book-links/isbn-clusters.parquet".into(), default())?;
  let icl = icl.select(&[col("isbn_id"), col("cluster")]);
  info!("reading edition IDs");
  let edl = LazyFrame::scan_parquet("openlibrary/edition-isbn-ids.parquet".into(), default())?;
  let edl = edl.filter(col("isbn_id").is_not_null());
  info!("reading edition authors");
  let mut eau = LazyFrame::scan_parquet("openlibrary/edition-authors.parquet".into(), default())?;
  if first_only {
    eau = eau.filter(col("pos").eq(0i16));
  }

  info!("reading author names");
  let auth = LazyFrame::scan_parquet("openlibrary/author-names.parquet".into(), default())?;
  let linked = icl.join(edl, [col("isbn_id")], [col("isbn_id")], JoinType::Inner);
  let linked = linked.join(eau, [col("edition")], [col("edition")], JoinType::Inner);
  let linked = linked.join(auth, [col("author")], [col("id")], JoinType::Inner);
  let authors = linked.select(vec![
    col("cluster"),
    col("name").alias("author_name").map(udf_clean_name, GetOutput::from_type(DataType::Utf8)),
  ]);

  Ok(authors)
}

/// Scan the Library of Congress data for first authors.
fn scan_loc(first_only: bool) -> Result<LazyFrame> {
  if !first_only {
    error!("only first-author extraction is currently supported");
    return Err(anyhow!("cannot extract multiple authors"));
  }

  info!("reading ISBN clusters");
  let icl = LazyFrame::scan_parquet("book-links/isbn-clusters.parquet".into(), default())?;
  let icl = icl.select([col("isbn_id"), col("cluster")]);

  info!("reading book records");
  let books = LazyFrame::scan_parquet("loc-mds/book-isbn-ids.parquet".into(), default())?;

  info!("reading book authors");
  let authors = LazyFrame::scan_parquet("loc-mds/book-authors.parquet".into(), default())?;
  let authors = authors.filter(col("author_name").is_not_null());

  let linked = icl.join(books, [col("isbn_id")], [col("isbn_id")], JoinType::Inner);
  let linked = linked.join(authors, [col("rec_id")], [col("rec_id")], JoinType::Inner);
  let authors = linked.select(vec![
    col("cluster"),
    col("author_name").map(udf_clean_name, GetOutput::from_type(DataType::Utf8)),
  ]);

  Ok(authors)
}

impl Command for ClusterAuthors {
  fn exec(&self) -> Result<()> {
    let mut authors: Option<LazyFrame> = None;
    for source in &self.sources {
      let astr = match source {
        Source::OpenLib => scan_openlib(self.first_author)?,
        Source::LOC => scan_loc(self.first_author)?,
      };
      debug!("author source {} has schema {:?}", source, astr.schema());
      if let Some(adf) = authors {
        authors = Some(concat([adf, astr], false)?);
      } else {
        authors = Some(astr);
      }
    }
    let authors = authors.ok_or(anyhow!("no sources specified"))?;
    let authors = authors.filter(
      col("author_name").is_not_null()
      .and(col("author_name").neq("".lit()))
    );
    let authors = authors.sort_by_exprs([
      col("cluster"),
      col("author_name"),
    ], vec![false, false]);

    // now we're going to de-duplicate - we can do this within:
    let authors = authors.filter(
      col("cluster").neq(col("cluster").shift(1))
      .or(col("author_name").neq(col("author_name").shift(1)))
    );

    debug!("plan: {}", authors.describe_plan());

    info!("collecting results");
    let mut authors = authors.collect()?;
    info!("found {} cluster-author links", authors.height());

    debug!("rechunking author table");
    authors.rechunk();

    info!("saving to {:?}", &self.output);
    // clean up nullability
    // we do the writing ourself because we have no nulls
    let mut schema = authors.schema().to_arrow();
    schema.fields[0].is_nullable = true;
    schema.fields[1].is_nullable = true;

    let writer = File::create(&self.output)?;
    let writer = FileWriter::try_new(writer, schema, WriteOptions {
      compression: ParquetCompression::Zstd(None),
      version: Version::V2,
      write_statistics: false,
    })?;
    let mut writer = ThreadObjectWriter::new(writer);
    for chunk in authors.iter_chunks() {
      writer.write_object(chunk)?;
    }
    writer.finish()?;

    info!("output file is {}", friendly::bytes(file_size(&self.output)?));

    Ok(())
  }
}
