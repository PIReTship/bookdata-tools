//! Helpers for DataFusion.
use std::io::Write;
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;

use futures::future;
use futures::stream::StreamExt;

use anyhow::Result;

use arrow::datatypes::DataType;
use arrow::array::*;
use datafusion::prelude::*;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::error::{Result as FusionResult, DataFusionError};
use parquet::arrow::ArrowWriter;
use parquet::file::writer::ParquetWriter;

use crate::cleaning::strings::norm_unicode;

/// Evaluate a DataFusion plan to a CSV file.
pub async fn eval_to_csv<W: Write>(out: &mut arrow::csv::Writer<W>, plan: Arc<dyn ExecutionPlan>) -> Result<()> {
  let plan = if plan.output_partitioning().partition_count() > 1 {
    Arc::new(MergeExec::new(plan.clone()))
  } else {
    plan
  };
  let mut batches = plan.execute(0).await?;
  while let Some(batch) = batches.next().await {
    let batch = batch?;
    out.write(&batch)?;
  }
  Ok(())
}

/// Evaluate a DataFusion plan to a Parquet file.
pub async fn eval_to_parquet<W: ParquetWriter + 'static>(out: &mut ArrowWriter<W>, plan: Arc<dyn ExecutionPlan>) -> Result<()> {
  let plan = if plan.output_partitioning().partition_count() > 1 {
    Arc::new(MergeExec::new(plan.clone()))
  } else {
    plan
  };
  let mut batches = plan.execute(0).await?;
  while let Some(batch) = batches.next().await {
    let batch = batch?;
    out.write(&batch)?;
  }
  Ok(())
}

/// Plan a data frame.
pub fn plan_df(ctx: &mut ExecutionContext, df: Arc<dyn DataFrame>) -> Result<Arc<dyn ExecutionPlan>> {
  let plan = df.to_logical_plan();
  let plan = ctx.optimize(&plan)?;
  let plan = ctx.create_physical_plan(&plan)?;
  let plan = if plan.output_partitioning().partition_count() > 1 {
    Arc::new(MergeExec::new(plan))
  } else {
    plan
  };
  Ok(plan)
}

/// Run a plan and get a stream of record batches.
pub fn run_plan<'a, 'async_trait>(plan: &'a Arc<dyn ExecutionPlan>) -> Pin<Box<dyn Future<Output = FusionResult<SendableRecordBatchStream>> + Send + 'async_trait>> where 'a: 'async_trait {
  if plan.output_partitioning().partition_count() != 1 {
    Box::pin(future::err(DataFusionError::Execution("plan has multiple partitions".to_string())))
  } else {
    plan.execute(0)
  }
}

/// UDF to normalize strings' Unicode representations.
fn udf_norm_unicode(args: &[ArrayRef]) -> datafusion::error::Result<ArrayRef> {
  let strs = &args[0].as_any().downcast_ref::<StringArray>().expect("invalid array cast");
  let res = strs.iter().map(|s| s.map(norm_unicode)).collect::<StringArray>();
  Ok(Arc::new(res) as ArrayRef)
}

/// Add our UDFs.
pub fn add_udfs(ctx: &mut ExecutionContext) {
  let norm = create_udf(
    "norm_unicode",
    vec![DataType::Utf8], Arc::new(DataType::Utf8),
    make_scalar_function(udf_norm_unicode));
  ctx.register_udf(norm);
}
