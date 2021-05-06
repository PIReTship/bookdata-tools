use std::path::{PathBuf};
use std::time::Instant;
use std::fs::{File, read_to_string};
use std::future::Future;
use std::sync::Arc;

use tokio;
use tokio::runtime::Runtime;

use bookdata::prelude::*;
use bookdata::arrow::fusion::*;

use flate2::write::GzEncoder;
use molt::*;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use datafusion::prelude::*;
use datafusion::physical_plan::{ExecutionPlan};

/// Run a DataFusion script and save its results.
///
/// This interprets small TCL scripts that drive DataFusion to process
/// data.
#[derive(StructOpt, Debug)]
#[structopt(name="fusion")]
pub struct Fusion {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Operation specification to run.
  #[structopt(name = "SCRIPT", parse(from_os_str))]
  script: PathBuf
}

struct ScriptContext {
  runtime: Runtime,
  df_context: ExecutionContext
}

impl ScriptContext {
  /// Initialize a new script context.
  fn create() -> Result<ScriptContext> {
    let runtime = Runtime::new()?;
    let mut df_context = ExecutionContext::new();
    df_context.register_udaf(Median::udaf());
    Ok(ScriptContext {
      runtime, df_context
    })
  }

  /// Run an asynchronous task.
  fn run<F: Future>(&self, task: F) -> <F as Future>::Output {
    self.runtime.block_on(task)
  }
}

/// Save an execution plan results to a Parquet directory.
fn save_parquet(ctx: &ScriptContext, plan: Arc<dyn ExecutionPlan>, file: &str) -> Result<()> {
  let props = WriterProperties::builder();
  let props = props.set_compression(Compression::ZSTD);
  let props = props.build();
  ctx.run(ctx.df_context.write_parquet(plan, file.to_owned(), Some(props)))?;
  Ok(())
}

/// Save an execution plan results to a CSV file.
fn save_csv(ctx: &ScriptContext, plan: Arc<dyn ExecutionPlan>, file: &str) -> Result<()> {
  info!("saving to CSV file");
  let out = File::create(file)?;
  let mut csvw = arrow::csv::WriterBuilder::new().has_headers(true).build(out);

  ctx.run(eval_to_csv(&mut csvw, plan))?;
  Ok(())
}

fn save_csvgz(ctx: &ScriptContext, plan: Arc<dyn ExecutionPlan>, file: &str) -> Result<()> {
  info!("saving to compressed CSV file");
  let out = File::create(file)?;
  let out = GzEncoder::new(out, flate2::Compression::best());
  let mut csvw = arrow::csv::WriterBuilder::new().has_headers(true).build(out);

  ctx.run(eval_to_csv(&mut csvw, plan))?;
  Ok(())
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
  wrap_errs(|| {
    info!("mounting table {} from {}", table, file);
    if file.ends_with(".parquet") {
      ctx.df_context.register_parquet(table, file)?;
    } else if file.ends_with(".csv") {
      ctx.df_context.register_csv(table, file, CsvReadOptions::new().has_header(true))?;
    }
    Ok(())
  })
}

/// Save results from a query.
fn cmd_save_results(interp: &mut Interp, ctx: ContextID, argv: &[Value]) -> MoltResult {
  check_args(1, argv, 3, 3, "file query")?;
  let ctx: &mut ScriptContext = interp.context(ctx);

  let file = argv[1].as_str();
  let query = argv[2].as_str();

  wrap_errs(|| {
    info!("planning query");
    debug!("query text: {}", query);
    let lplan = ctx.df_context.create_logical_plan(&query)?;
    let plan = ctx.df_context.create_physical_plan(&lplan)?;
    debug!("query plan: {:?}", plan);

    info!("executing script to file {}", file);

    if file.ends_with(".parquet") {
      save_parquet(ctx, plan, file)?;
    } else if file.ends_with(".csv") {
      save_csv(ctx, plan, file)?;
    } else if file.ends_with(".csv.gz") {
      save_csvgz(ctx, plan, file)?;
    } else {
      return Err(anyhow!("unknown suffix in file {}", file));
    }
    Ok(())
  })
}

pub fn main() -> Result<()> {
  let opts = Fusion::from_args();
  opts.common.init()?;

  let ctx = ScriptContext::create()?;
  let mut interp = Interp::new();
  let scid = interp.save_context(ctx);
  interp.add_context_command("table", cmd_table, scid);
  interp.add_context_command("save-results", cmd_save_results, scid);

  info!("reading script from {}", &opts.script.to_string_lossy());
  let script = read_to_string(&opts.script)?;

  info!("evaluating script");
  let start = Instant::now();
  if let Err(e) = interp.eval(&script) {
    error!("error running script: {:?}", e);
    Err(anyhow!("TCL error {}: {}", e.error_code().as_str(), e.error_info().as_str()))
  } else {
    info!("script completed successfully in {}",
          human_time(start.elapsed()));
    Ok(())
  }
}
