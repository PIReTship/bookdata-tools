//! Helpers for DataFusion.
use std::io::Write;
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;

use futures::future;
use futures::stream::{Stream, StreamExt};
use serde::de::DeserializeOwned;

use anyhow::Result;

use arrow::datatypes::DataType;
use arrow::array::*;
use datafusion::prelude::*;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::ColumnarValue;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::error::{Result as FusionResult, DataFusionError};
use parquet::arrow::ArrowWriter;
use parquet::file::writer::ParquetWriter;

use crate::cleaning::strings::norm_unicode;
use super::row_de::RecordBatchDeserializer;

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

/// Deserialize rows of a data frame as a stream.
pub async fn df_rows<R>(ctx: &mut ExecutionContext, df: Arc<dyn DataFrame>) -> Result<impl Stream<Item=Result<R>>>
where R: DeserializeOwned
{
  let plan = plan_df(ctx, df)?;
  let stream = run_plan(&plan).await?;
  Ok(RecordBatchDeserializer::for_stream(stream))
}

/// UDF to normalize strings' Unicode representations.
fn udf_norm_unicode(args: &[ArrayRef]) -> datafusion::error::Result<ArrayRef> {
  let strs = &args[0].as_any().downcast_ref::<StringArray>().expect("invalid array cast");
  let res = strs.iter().map(|s| s.map(norm_unicode)).collect::<StringArray>();
  Ok(Arc::new(res) as ArrayRef)
}

/// UDF to implement fillna (a limited version of COALESCE)
fn udf_fillna(args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
  assert_eq!(args.len(), 2);
  let nargs = args.len();
  let mut arrays = Vec::with_capacity(nargs);
  let mut n = 0;
  for i in 0..nargs {
    let a = match &args[i] {
      ColumnarValue::Array(arr) => arr.clone(),
      ColumnarValue::Scalar(s) => s.to_array()
    };
    if a.len() > n {
      n = a.len();
    }
    arrays.push(a);
  }
  let arefs: Vec<&StringArray> = arrays.iter().map(|a| {
    a.as_any().downcast_ref::<StringArray>().expect("invalid array cast")
  }).collect();

  let mut res = StringBuilder::new(arefs[0].get_array_memory_size());

  for i in 0..n {
    let mut added = false;
    for a in &arefs {
      let ai = if a.len() > 1 {
        i
      } else {
        1
      };
      if a.is_valid(ai) {
        res.append_value(a.value(ai))?;
        added = true;
      }
    }

    if !added {
      res.append_null()?;
    }
  }

  Ok(ColumnarValue::Array(Arc::new(res.finish()) as ArrayRef))
}

/// Add our UDFs.
pub fn add_udfs(ctx: &mut ExecutionContext) {
  let norm = create_udf(
    "norm_unicode",
    vec![DataType::Utf8], Arc::new(DataType::Utf8),
    make_scalar_function(udf_norm_unicode));
  ctx.register_udf(norm);

  let fillna = create_udf(
    "fillna",
    vec![DataType::Utf8, DataType::Utf8],
    Arc::new(DataType::Utf8),
    Arc::new(udf_fillna));
  ctx.register_udf(fillna);
}
