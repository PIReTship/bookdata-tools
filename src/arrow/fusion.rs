use std::io::Write;
use std::sync::Arc;

use futures::stream::StreamExt;

use anyhow::Result;

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
