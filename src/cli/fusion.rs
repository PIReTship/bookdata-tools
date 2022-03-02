//! DataFusion script runner.
use std::path::{PathBuf};
use std::fs::{File, read_to_string};
use std::future::Future;
use std::sync::Arc;

use futures::stream::{StreamExt};
use tokio;
use tokio::runtime::Runtime;

use crate::prelude::*;
use crate::arrow::fusion::*;

use flate2::write::GzEncoder;
use molt::*;
use arrow::record_batch::RecordBatch;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::arrow::ArrowWriter;
use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};

/// Run a DataFusion script and save its results.
///
/// This interprets small TCL scripts that drive DataFusion to process
/// data.
#[derive(StructOpt, Debug)]
#[structopt(name="fusion")]
pub struct Fusion {
  /// Operation specification to run.
  #[structopt(name = "SCRIPT", parse(from_os_str))]
  script: PathBuf
}

struct ScriptContext {
  runtime: Runtime,
  df_context: ExecutionContext,
}

impl ScriptContext {
  /// Initialize a new script context.
  fn create() -> Result<ScriptContext> {
    let runtime = Runtime::new()?;
    let mut df_context = ExecutionContext::new();
    add_udfs(&mut df_context);
    Ok(ScriptContext {
      runtime, df_context
    })
  }
}

/// Save an execution plan to a writer.
async fn write_plan_results<W>(ctx: &ExecutionContext, plan: Arc<dyn ExecutionPlan>, mut out: W) -> Result<()>
  where W: for <'a> ObjectWriter<&'a RecordBatch>
{
  let runtime = ctx.runtime_env();
  let mut batches = execute_stream(plan, runtime).await?;
  while let Some(batch) = batches.next().await {
    let batch = batch?;
    out.write_object(&batch)?;
  }

  out.finish()?;

  Ok(())
}

/// Save an execution plan results to a Parquet directory.
///
/// The weird lifetime on this function is to handle using a reference in an asynchonous function.
/// Rust doesn't naturally support those very well.
fn save_parquet<'x>(ctx: &'x ExecutionContext, plan: Arc<dyn ExecutionPlan>, file: &str, partitioned: bool) -> impl Future<Output=Result<()>> + 'x {
  // take ownership of the file
  let file = file.to_string();
  // set up the writer
  let props = WriterProperties::builder();
  let props = props.set_compression(Compression::ZSTD);
  let props = props.set_dictionary_enabled(false);
  let props = props.build();

  async move {
    if partitioned {
      ctx.write_parquet(plan, file.to_owned(), Some(props)).await?;
    } else {
      let file = File::create(file)?;
      let write = ArrowWriter::try_new(file, plan.schema(), Some(props))?;
      write_plan_results(ctx, plan, write).await?;
    }

    Ok(())
  }
}

/// Save an execution plan results to a CSV file.
async fn save_csv(ctx: &ExecutionContext, plan: Arc<dyn ExecutionPlan>, file: &str) -> Result<()> {
  info!("saving to CSV file");
  // take ownership of the filename string
  let file = file.to_string();

  // set up the output
  let out = File::create(file)?;
  let csvw = arrow::csv::WriterBuilder::new().has_headers(true).build(out);
  write_plan_results(ctx, plan, csvw).await?;

  Ok(())
}

async fn save_csvgz(ctx: &ExecutionContext, plan: Arc<dyn ExecutionPlan>, file: &str) -> Result<()> {
  info!("saving to compressed CSV file");
  // take ownership of the filename string
  let file = file.to_string();

  // set up the output
  let out = File::create(file)?;
  let out = GzEncoder::new(out, flate2::Compression::best());
  let csvw = arrow::csv::WriterBuilder::new().has_headers(true).build(out);
  write_plan_results(ctx, plan, csvw).await?;

  Ok(())
}

/// Helper function to wrap asynchronous tasks with errors in Molt
fn async_wrap_errs<F, T>(rt: &Runtime, task: F) -> MoltResult where F: Future<Output=Result<T>> {
  wrap_errs(|| {
    rt.block_on(task)
  })
}

/// Helper function to wrap Rust errors in Molt
fn wrap_errs<P, T>(proc: P) -> MoltResult where P: FnOnce() -> Result<T> {
  let res = proc();
  if let Err(e) = res {
    error!("error in Rust command body: {:?}", e);
    molt_err!("Rust error: {}", e)
  } else {
    molt_ok!()
  }
}

/// Bind a table.
fn cmd_table(interp: &mut Interp, ctx: ContextID, argv: &[Value]) -> MoltResult {
  check_args(1, argv, 3, 3, "name file")?;

  let ctx: &mut ScriptContext = interp.context(ctx);
  let table = argv[1].as_str();
  let file = argv[2].as_str();
  async_wrap_errs(&ctx.runtime, async {
    info!("mounting table {} from {}", table, file);
    if file.ends_with(".parquet") {
      ctx.df_context.register_parquet(table, file).await?;
    } else if file.ends_with(".csv") {
      ctx.df_context.register_csv(table, file, CsvReadOptions::new().has_header(true)).await?;
    } else {
      error!("unknown table type");
      return Err(anyhow!("{} has unkown table type", file));
    }
    Ok(())
  })
}

/// Save results from a query.
fn cmd_save_results(interp: &mut Interp, ctx: ContextID, argv: &[Value]) -> MoltResult {
  check_args(1, argv, 3, 4, "?-partitioned? file query")?;
  let ctx: &mut ScriptContext = interp.context(ctx);

  let mut partitioned = false;
  let mut file = argv[1].as_str();
  let mut query = argv[2].as_str();
  if file == "-partitioned" {
    partitioned = true;
    file = query;
    query = argv[3].as_str();
  }

  async_wrap_errs(&ctx.runtime, async {
    log_exc_info(&ctx.df_context)?;
    info!("planning query");
    debug!("query text: {}", query);
    let lplan = ctx.df_context.create_logical_plan(&query)?;
    let lplan = ctx.df_context.optimize(&lplan)?;
    let plan = ctx.df_context.create_physical_plan(&lplan).await?;
    debug!("query plan: {:?}", plan);

    info!("executing script to file {}", file);

    if file.ends_with(".parquet") {
      save_parquet(&ctx.df_context, plan, file, partitioned).await?;
    } else if file.ends_with(".csv") {
      save_csv(&ctx.df_context, plan, file).await?;
    } else if file.ends_with(".csv.gz") {
      save_csvgz(&ctx.df_context, plan, file).await?;
    } else {
      return Err(anyhow!("unknown suffix in file {}", file));
    }
    Ok(())
  })
}

/// Run a query and show its results.
fn cmd_query(interp: &mut Interp, ctx: ContextID, argv: &[Value]) -> MoltResult {
  check_args(1, argv, 2, 2, "query")?;
  let ctx: &mut ScriptContext = interp.context(ctx);

  let query = argv[1].as_str();

  async_wrap_errs(&ctx.runtime, async {
    log_exc_info(&ctx.df_context)?;
    info!("preparing query");
    debug!("query text: {}", query);

    let df = ctx.df_context.sql(query).await?;
    let res = df.collect().await?;
    debug!("finished with {} batches", res.len());
    println!("{}", pretty_format_batches(&res)?);

    Ok(())
  })
}


impl Command for Fusion {
  fn exec(&self) -> Result<()> {
    let ctx = ScriptContext::create()?;
    let mut interp = Interp::new();
    let scid = interp.save_context(ctx);
    interp.add_context_command("table", cmd_table, scid);
    interp.add_context_command("save-results", cmd_save_results, scid);
    interp.add_context_command("query", cmd_query, scid);

    info!("reading script from {}", self.script.to_string_lossy());
    let script = read_to_string(&self.script)?;

    info!("evaluating script");
    let start = Timer::new();
    if let Err(e) = interp.eval(&script) {
      error!("error running script: {:?}", e);
      Err(anyhow!("TCL error {}: {}", e.error_code().as_str(), e.error_info().as_str()))
    } else {
      info!("script completed successfully in {}",
            start.human_elapsed());
      Ok(())
    }
  }
}
