//! Command line interface for book data.
//!
//! The book data tools are implemented as a single monolithic executable (to reduce
//! compilation time and disk space in common configurations, with different tools
//! implemented as subcommands.  Each subcommand implements the [Command] trait, which
//! exposes the command line arguments and invocation.
pub mod fusion;
pub mod scan_marc;
pub mod index_names;
pub mod extract_graph;
pub mod cluster_books;
pub mod cluster;
pub mod collect_isbns;
pub mod amazon;
pub mod openlib;
pub mod goodreads;
pub mod pqinfo;

use std::env;
use log::*;
use paste::paste;
use structopt::StructOpt;
use anyhow::Result;
use tokio::runtime::Runtime;
use enum_dispatch::enum_dispatch;
use async_trait::async_trait;
use cpu_time::ProcessTime;
use pretty_env_logger::formatted_timed_builder;

#[cfg(unix)]
use crate::util::process;

/// Macro to generate wrappers for subcommand enums.
///
/// This is for subcommands that only exist to contain further subcommands,
/// to make it easier to implement their wrapper classes.
macro_rules! wrap_subcommands {
  ($name:ty) => {
    paste! {
      #[derive(StructOpt, Debug)]
      pub struct [<$name Wrapper>] {
        #[structopt(subcommand)]
        command: $name
      }

      impl Command for [<$name Wrapper>] {
        fn exec(&self) -> Result<()> {
          self.command.exec()
        }
      }
    }
  };
}

/// Trait implemented by book data commands.
#[enum_dispatch]
pub trait Command {
  /// Run the command with options
  fn exec(&self) -> Result<()>;
}

/// Enum to collect and dispatch CLI commands.
#[enum_dispatch(Command)]
#[derive(StructOpt, Debug)]
pub enum BDCommand {
  Fusion(fusion::Fusion),
  ScanMARC(scan_marc::ScanMARC),
  ClusterBooks(cluster_books::ClusterBooks),
  IndexNames(index_names::IndexNames),
  ExtractGraph(extract_graph::ExtractGraph),
  CollectISBNS(collect_isbns::CollectISBNs),
  /// Commands for processing Amazon data.
  Amazon(AmazonCommandWrapper),
  /// Commands for processing OpenLibrary data,
  Openlib(openlib::OpenLib),
  Goodreads(goodreads::Goodreads),
  /// Commands for working with clusters.
  Cluster(ClusterCommandWrapper),
  PQInfo(pqinfo::PQInfo),
}

wrap_subcommands!(AmazonCommand);
wrap_subcommands!(ClusterCommand);

#[enum_dispatch(Command)]
#[derive(StructOpt, Debug)]
enum AmazonCommand {
  ScanRatings(amazon::ScanRatings),
}

#[enum_dispatch(Command)]
#[derive(StructOpt, Debug)]
pub enum ClusterCommand {
  Hash(cluster::hash::HashCmd),
  ExtractAuthors(cluster::authors::ClusterAuthors),
  ExtractAuthorGender(cluster::author_gender::AuthorGender),
  GroupActions(cluster::actions::ClusterActions),
}

/// Trait for implementing commands asynchronously.
#[async_trait]
pub trait AsyncCommand {
  async fn exec_future(&self) -> Result<()>;
}

#[async_trait]
impl <T: AsyncCommand> Command for T {
  fn exec(&self) -> Result<()> {
    let runtime = Runtime::new()?;
    let task = self.exec_future();
    runtime.block_on(task)
  }
}

/// Entry point for the Book Data Tools.
///
/// This program runs the various book data tools, exposed as subcommands.  The top-level
/// command handles setting up logging.  Logging can be configured in detail with the
/// environment variable `BOOKDATA_LOG`, using the `env_logger` crate; the command
/// line options provide shortcuts but are overridden by the environment variable.
#[derive(StructOpt, Debug)]
#[structopt(name="bookdata")]
pub struct CLI {
  /// Verbose mode (-v, -vv, -vvv, etc.)
  #[structopt(short="v", long="verbose", parse(from_occurrences))]
  verbose: usize,

  /// Silence output (overrides -v)
  #[structopt(short="q", long="quiet")]
  quiet: bool,

  #[structopt(subcommand)]
  command: BDCommand,
}

impl CLI {
  pub fn exec(self) -> Result<()> {
    let mut lb = formatted_timed_builder();
    if self.quiet {
      lb.filter_level(LevelFilter::Error);
    } else if self.verbose == 1 {
      lb.filter_level(LevelFilter::Debug);
      lb.filter_module("bookdata", LevelFilter::Info);
    } else if self.verbose == 2 {
      lb.filter_level(LevelFilter::Debug);
    } else if self.verbose >= 3 {
      lb.filter_level(LevelFilter::Trace);
    } else {
      lb.filter_level(LevelFilter::Info);
    }
    if let Ok(filt) = env::var("BOOKDATA_LOG") {
      lb.parse_filters(&filt);
    }
    lb.try_init()?;

    let res = self.command.exec();

    match ProcessTime::try_now() {
      Ok(pt) => info!("used {} CPU time", friendly::duration(pt.as_duration())),
      Err(e) => error!("error fetching CPU time: {}", e)
    };
    #[cfg(unix)]
    process::log_process_stats();
    res
  }
}
