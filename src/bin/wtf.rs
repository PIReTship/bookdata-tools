use std::fs::File;

// use tokio;
use polars::prelude::*;

// use bookdata::prelude::*;

fn main() -> Result<()> {
  let file = File::open("openlibrary/works.parquet")?;
  let df = ParquetReader::new(file).finish()?;
  let mask = df.column("key")?.eq("/works/OL8193418W");
  let df = df.filter(&mask)?;
  println!("{}", df);

  Ok(())
}
