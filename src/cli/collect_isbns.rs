use std::fmt::Debug;
use std::sync::Arc;
use std::collections::HashMap;
use futures::StreamExt;
use datafusion::prelude::*;
use paste::paste;

use serde::Deserialize;
use async_trait::async_trait;

use crate::prelude::*;
use crate::arrow::*;
use crate::arrow::row_de::RecordBatchDeserializer;

use super::AsyncCommand;

/// Collect ISBNs from across the data sources.
#[derive(StructOpt, Debug)]
#[structopt(name="collect-isbns")]
pub struct CollectISBNs {
}

#[derive(TableRow, Debug, Default, Clone)]
struct ISBNRecord {
  isbn: String,
  isbn_id: i32,
  loc_recs: i32,
  ol_recs: i32,
  gr_recs: i32,
  bx_recs: i32,
  az14_recs: i32,
  az18_recs: i32,
}

#[derive(Deserialize)]
struct ISBN {
  isbn: String
}

type Accum = HashMap<String, ISBNRecord>;

trait ISBNSrc where Self: Debug {
  fn record(acc: &mut Accum, isbn: String);
}

macro_rules! make_accumulator {
  ($src:ident, $ty:ident) => {
    paste! {
      #[derive(Debug)]
      struct $ty;

      impl ISBNSrc for $ty {
        fn record(acc: &mut Accum, isbn: String) {
          let n = acc.len() as i32;
          let mut obj = acc.entry(isbn.clone()).or_insert_with(|| {
            let mut r = ISBNRecord::default();
            r.isbn = isbn;
            r.isbn_id = n + 1;
            r
          });
          obj.[<$src _recs>] += 1;
        }
      }

    }
  };
}

make_accumulator!(loc, LOC);
make_accumulator!(ol, OL);
make_accumulator!(gr, GR);
make_accumulator!(bx, BX);
make_accumulator!(az14, AZ14);
make_accumulator!(az18, AZ18);

async fn read_loc(ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
  let df = ctx.read_parquet("loc-mds/book-isbns.parquet").await?;
  let df = df.select_columns(&["isbn"])?;
  let df = df.filter(col("isbn").is_not_null())?;
  Ok(df)
}

async fn read_ol(ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
  let df = ctx.read_parquet("openlibrary/edition-isbns.parquet").await?;
  let df = df.select_columns(&["isbn"])?;
  let df = df.filter(col("isbn").is_not_null())?;
  Ok(df)
}

async fn read_gr(ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
  let df = ctx.read_parquet("goodreads/gr-book-ids.parquet").await?;
  let df_10 = df.select(vec![
    col("isbn10").alias("isbn")
  ])?.filter(col("isbn").is_not_null())?;
  let df_13 = df.select(vec![
    col("isbn13").alias("isbn")
  ])?.filter(col("isbn").is_not_null())?;
  let df_az = df.select(vec![
    col("asin").alias("isbn")
  ])?.filter(col("isbn").is_not_null())?;
  let df = df_10.union(df_13)?.union(df_az)?;
  Ok(df)
}

async fn read_bx(ctx: &mut ExecutionContext) -> Result<Arc<dyn DataFrame>> {
  let opts = CsvReadOptions::new().has_header(true);
  let df = ctx.read_csv("bx/cleaned-ratings.csv", opts).await?;
  let df = df.select_columns(&["isbn"])?;
  let df = df.filter(col("isbn").is_not_null())?;
  Ok(df)
}

async fn read_az(ctx: &mut ExecutionContext, dir: &str) -> Result<Arc<dyn DataFrame>> {
  let path = format!("{}/ratings.parquet", dir);
  let df = ctx.read_parquet(path).await?;
  let df = df.select(vec![
    col("asin").alias("isbn")
  ])?;
  let df = df.filter(col("isbn").is_not_null())?;
  Ok(df)
}

async fn record_isbns<T: ISBNSrc>(acc: &mut Accum, df: Arc<dyn DataFrame>, ty: T) -> Result<()> {
  info!("recording ISBNs from {:?}", ty);

  let stream = df.execute_stream().await?;
  let mut rec_stream = RecordBatchDeserializer::for_stream(stream);
  while let Some(row) = rec_stream.next().await {
    let row: ISBN = row?;
    T::record(acc, row.isbn);
  }

  Ok(())
}

#[async_trait]
impl AsyncCommand for CollectISBNs {
  async fn exec_future(&self) -> Result<()> {
    let mut acc = Accum::new();
    let mut ctx = ExecutionContext::new();

    record_isbns(&mut acc, read_loc(&mut ctx).await?, LOC).await?;
    record_isbns(&mut acc, read_ol(&mut ctx).await?, OL).await?;
    record_isbns(&mut acc, read_gr(&mut ctx).await?, GR).await?;
    record_isbns(&mut acc, read_bx(&mut ctx).await?, BX).await?;
    record_isbns(&mut acc, read_az(&mut ctx, "az2014").await?, AZ14).await?;
    record_isbns(&mut acc, read_az(&mut ctx, "az2018").await?, AZ18).await?;

    info!("found {} distinct ISBNs", acc.len());

    let mut writer = TableWriter::open("book-links/all-isbns.parquet")?;
    for ir in acc.values() {
      writer.write_object(ir.clone())?;
    }
    writer.finish()?;

    info!("wrote ISBNs to book-links/all-isbns.parquet");

    Ok(())
  }
}
