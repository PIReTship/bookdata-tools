use std::fs::File;
use std::convert::From;
use std::path::PathBuf;
use std::collections::HashSet;

use tokio;

use datafusion::prelude::*;
use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::graph::{BookID, IdGraph, IdNode, load_graph, save_gml};
use bookdata::ids::codes::{NS_ISBN, ns_of_book_code};

use serde::Serialize;

mod openlib;

/// Extract book information.
#[derive(StructOpt, Debug)]
#[structopt(name="book-info")]
pub struct BookInfo {
  #[structopt(flatten)]
  common: CommonOpts,

  #[structopt(short="b", long="book")]
  book_code: Option<i32>,

  #[structopt(short="c", long="cluster")]
  cluster: Option<i32>,
}

/// Get information for book codes.
async fn info_for_code(ctx: &mut ExecutionContext, code: i32) -> Result<()> {
  let ns = ns_of_book_code(code).ok_or(anyhow!("invalid book code"))?;
  info!("code {} is in numspace {}", code, ns.name);

  if ns.name == "OL-W" {
    openlib::work_info(ctx, ns.from_code(code).unwrap()).await?;
  } else {
    warn!("what is this code?");
  }

  Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
  let opts = BookInfo::from_args();
  opts.common.init()?;

  let mut ctx = ExecutionContext::new();

  if let Some(bc) = opts.book_code {
    info_for_code(&mut ctx, bc).await?;
  } else {
    error!("no book specified");
    std::process::exit(2);
  }

  Ok(())
}
