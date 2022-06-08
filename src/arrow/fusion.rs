//! Helpers for DataFusion.
use std::sync::Arc;

use datafusion::logical_expr::ScalarUDF;
use log::*;

use futures::stream::{Stream};
use serde::de::DeserializeOwned;

use anyhow::Result;
use lazy_static::lazy_static;

use arrow::datatypes::*;
use arrow::array::*;
use datafusion::prelude::*;
use datafusion::physical_plan::functions::{make_scalar_function};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::logical_expr::{Volatility};

use crate::ids::codes;
use crate::cleaning::strings::norm_unicode;
use crate::cleaning::names::clean_name;
use super::row_de::RecordBatchDeserializer;

/// Log basic info about an execution context.
pub fn log_exc_info(exc: &SessionContext) -> Result<()> {
  let state = exc.state.read();
  debug!("catalogs: {:?}", state.catalog_list.catalog_names());
  let cp = state.catalog_list.catalog("datafusion").unwrap();
  debug!("schemas in datafusion: {:?}", cp.schema_names());
  let tables = cp.schema("public").unwrap().table_names();
  info!("have {} registered tables", tables.len());
  for t in &tables {
    debug!("table: datafusion.public.{}", t);
  }
  Ok(())
}

/// Plan a data frame.
pub async fn plan_df(ctx: &mut SessionContext, df: Arc<DataFrame>) -> Result<Arc<dyn ExecutionPlan>> {
  let plan = df.to_logical_plan()?;
  let plan = ctx.optimize(&plan)?;
  let plan = ctx.create_physical_plan(&plan).await?;
  Ok(plan)
}

/// Deserialize rows of a data frame as a stream.
pub async fn df_rows<R>(df: Arc<DataFrame>) -> Result<impl Stream<Item=Result<R>>>
where R: DeserializeOwned
{
  let stream = df.execute_stream().await?;
  Ok(RecordBatchDeserializer::for_stream(stream))
}

/// UDF to normalize strings' Unicode representations.
fn udf_norm_unicode_impl(args: &[ArrayRef]) -> datafusion::error::Result<ArrayRef> {
  let strs = &args[0].as_any().downcast_ref::<StringArray>().expect("invalid array cast");
  let res = strs.iter().map(|s| s.map(norm_unicode)).collect::<StringArray>();
  Ok(Arc::new(res) as ArrayRef)
}

/// UDF to clean author name strings.
fn udf_clean_name_impl(args: &[ArrayRef]) -> datafusion::error::Result<ArrayRef> {
  let strs = &args[0].as_any().downcast_ref::<StringArray>().expect("invalid array cast");
  let res = strs.iter().map(|s| s.map(clean_name)).collect::<StringArray>();
  Ok(Arc::new(res) as ArrayRef)
}

lazy_static! {
  static ref UDF_NORM_UNICODE: ScalarUDF = create_udf(
    "norm_unicode",
    vec![DataType::Utf8], Arc::new(DataType::Utf8),
    Volatility::Immutable,
    make_scalar_function(udf_norm_unicode_impl));
  static ref UDF_CLEAN_NAME: ScalarUDF = create_udf(
    "clean_name",
    vec![DataType::Utf8], Arc::new(DataType::Utf8),
    Volatility::Immutable,
    make_scalar_function(udf_clean_name_impl));
}

/// [`clean_name`] as a DataFusion UDF for the expression DSL.
pub fn udf_norm_unicode(e: Expr) -> Expr {
  UDF_NORM_UNICODE.call(vec![e])
}

/// [`clean_name`] as a DataFusion UDF for the expression DSL.
pub fn udf_clean_name(e: Expr) -> Expr {
  UDF_CLEAN_NAME.call(vec![e])
}

/// Add our UDFs.
pub fn add_udfs(ctx: &mut SessionContext) {
  ctx.register_udf(UDF_NORM_UNICODE.clone());
  ctx.register_udf(UDF_CLEAN_NAME.clone());

  codes::add_udfs(ctx);
}
