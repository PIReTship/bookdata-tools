//! Helpers for DataFusion.
use std::io::Write;
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;

use futures::future;
use futures::stream::StreamExt;

use anyhow::Result;

use serde::{Serialize, Deserialize};
use bincode;
use encoding::{Encoding, DecoderTrap, EncoderTrap};
use encoding::all::ISO_8859_1;

use datafusion::prelude::*;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Accumulator;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::logical_plan::{create_udaf};
use datafusion::scalar::ScalarValue;
use datafusion::error::{Result as FusionResult, DataFusionError};
use arrow::datatypes::*;
use parquet::arrow::ArrowWriter;
use parquet::file::writer::ParquetWriter;

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


/// Median aggregate function.
#[derive(Debug, Serialize, Deserialize)]
pub struct Median {
  values: Vec<f32>
}

impl Median {
  /// Construct a new median.
  pub fn new() -> Self {
    Median { values: Vec::new() }
  }

  /// Get the UDAF definition for median.
  pub fn udaf() -> AggregateUDF {
    create_udaf(
      "median",
      DataType::Float32,
      Arc::new(DataType::Float32),
      Arc::new(|| Ok(Box::new(Median::new()))),
      Arc::new(vec![DataType::Utf8])
    )
  }
}

impl Accumulator for Median {
  fn state(&self) -> FusionResult<Vec<ScalarValue>> {
    let encoded: Vec<u8> = bincode::serialize(self).unwrap();
    // this is an absolutely terrible idea we only do because arrow binary doesn't work
    let encoded = ISO_8859_1.decode(&encoded, DecoderTrap::Strict).expect("yikes");
    Ok(vec![ScalarValue::Utf8(Some(encoded))])
  }

  fn update(&mut self, values: &[ScalarValue]) -> FusionResult<()> {
    match &values[0] {
      ScalarValue::Float32(Some(e)) => {
        self.values.push(*e);
      },
      ScalarValue::Float32(None) => (),
      _ => unreachable!("unexpected input type")
    }
    Ok(())
  }

  fn merge(&mut self, states: &[ScalarValue]) -> FusionResult<()> {
    let s1 = &states[0];
    match s1 {
      ScalarValue::Utf8(Some(chars)) => {
        let bytes = ISO_8859_1.encode(&chars, EncoderTrap::Strict).expect("yikes2");
        let vs: Vec<f32> = bincode::deserialize(&bytes).unwrap();
        self.values.extend_from_slice(&vs);
      },
      _ => unreachable!("invalid state")
    };
    Ok(())
  }

  fn evaluate(&self) -> FusionResult<ScalarValue> {
    let mut v2 = self.values.clone();
    v2.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if v2.len() % 2 == 1 {
      // odd, pick the middle
      let i = v2.len() / 2;
      Ok(ScalarValue::from(v2[i]))
    } else {
      let i = v2.len() / 2;
      Ok(ScalarValue::Float32(Some((v2[i-1] + v2[i]) * 0.5)))
    }
  }
}
