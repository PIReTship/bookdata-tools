use datafusion::prelude::*;
use bookdata::prelude::*;

use arrow::util::pretty::pretty_format_batches;

pub async fn work_info(ctx: &mut ExecutionContext, id: i32) -> Result<()> {
  info!("looking up OpenLibrary work {}", id);
  let df = ctx.read_parquet("openlibrary/works.parquet")?;
  let sel = df.filter(col("id").eq(lit(id)))?;
  // let sel = sel.select_columns(&["key", "title"])?;
  let plan = sel.to_logical_plan();
  debug!("logical plan:\n{:?}", plan);
  let res = sel.collect().await?;
  debug!("found {} batches", res.len());

  println!("{}", pretty_format_batches(&res)?);

  Ok(())
}
