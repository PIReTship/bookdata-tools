//! Helpers for DataFusion.
use std::io::Write;
use std::iter::Zip;
use std::sync::Arc;

use futures::stream::{Stream, StreamExt, self};

use anyhow::Result;

use arrow::datatypes::*;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::merge::MergeExec;

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

/// Plan a data frame.
pub fn plan_df(ctx: &mut ExecutionContext, df: Arc<dyn DataFrame>) -> Result<Arc<dyn ExecutionPlan>> {
  let plan = df.to_logical_plan();
  let plan = ctx.optimize(&plan)?;
  let plan = ctx.create_physical_plan(&plan)?;
  Ok(plan)
}

/// Convert one batch column to a stream
pub fn stream_batch_column<'a, T>(batch: &'a RecordBatch) -> stream::Iter<PrimitiveIter<'a, T>>
where
  T: ArrowPrimitiveType
{
  let col = batch.column(0);
  let col = col.as_any().downcast_ref::<PrimitiveArray<T>>().expect("failed downcast");
  stream::iter(col)
}

/// Convert a pair of batch columns to a stream
pub fn stream_batch_pair<'a, T1, T2>(batch: &'a RecordBatch) -> stream::Iter<Zip<PrimitiveIter<'a, T1>, PrimitiveIter<'a, T2>>>
where
  T1: ArrowPrimitiveType,
  T2: ArrowPrimitiveType
{
  let col1 = batch.column(0);
  let col1 = col1.as_any().downcast_ref::<PrimitiveArray<T1>>().expect("failed downcast");
  let col2 = batch.column(0);
  let col2 = col2.as_any().downcast_ref::<PrimitiveArray<T2>>().expect("failed downcast");
  stream::iter(col1.iter().zip(col2.iter()))
}
