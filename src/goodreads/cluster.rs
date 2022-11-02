//! Support for resolving GoodReads interactions to clusters.
use std::path::{Path, PathBuf};
use std::fs::File;

use crate::prelude::*;
use crate::ids::codes::{NS_GR_WORK, NS_GR_BOOK};

use polars::prelude::*;

#[derive(Debug, PartialEq, Eq)]
enum SrcType {
  Simple,
  Full,
}

#[derive(Debug, PartialEq, Eq)]
enum ActionType {
  Ratings,
  AddActions,
}

#[derive(Debug, PartialEq, Eq)]
enum AggType {
  Clusters,
  NativeWorks,
}

#[derive(Debug)]
pub struct ClusterOp {
  actions: ActionType,
  data: SrcType,
  clusters: AggType,
  output: PathBuf,
}

impl ClusterOp {
  /// Start a new action-clustering operation.
  pub fn add_actions<P: AsRef<Path>>(path: P) -> ClusterOp {
    ClusterOp {
      actions: ActionType::AddActions,
      data: SrcType::Full,
      clusters: AggType::Clusters,
      output: path.as_ref().to_path_buf(),
    }
  }

  /// Start a new rating-clustering operation.
  pub fn ratings<P: AsRef<Path>>(path: P) -> ClusterOp {
    ClusterOp {
      actions: ActionType::Ratings,
      data: SrcType::Full,
      clusters: AggType::Clusters,
      output: path.as_ref().to_path_buf(),
    }
  }

  /// Set operation to cluster simple records instead of full records.
  pub fn simple(self) -> ClusterOp {
    ClusterOp {
      data: SrcType::Simple,
      ..self
    }
  }

  /// Set operation to cluster with native works instead of clusters.
  pub fn native_works(self) -> ClusterOp {
    ClusterOp {
      clusters: AggType::NativeWorks,
      ..self
    }
  }

  /// Run the clustering operation.
  pub fn cluster(self) -> Result<()> {
    let interactions = self.load_input()?;
    let interactions = self.filter(interactions);
    let interactions = self.project_and_sort(interactions);
    let actions = interactions.clone().groupby(&[
      col("user"), col("item")
    ]).agg(self.aggregates());

    let actions = self.maybe_integrate_ratings(actions, &interactions);

    debug!("logical plan: {:?}", actions.describe_plan());
    debug!("optimized plan: {:?}", actions.describe_optimized_plan()?);
    info!("collecting results");
    let mut actions = actions.collect()?;

    info!("writing {} actions to {:?}", actions.height(), &self.output);
    let file = File::create(&self.output)?;
    ParquetWriter::new(file)
      .with_compression(ParquetCompression::Zstd(None))
      .with_row_group_size(Some(1000_000))
      .finish(&mut actions)?;

    Ok(())
  }

  /// Load the input.
  fn load_input(&self) -> PolarsResult<LazyFrame> {
    let dir = match self.data {
      SrcType::Full => "full",
      SrcType::Simple => "simple",
    };
    let path = format!("goodreads/{}/gr-interactions.parquet", dir);
    let data = LazyFrame::scan_parquet(path, Default::default())?;

    let links = LazyFrame::scan_parquet("goodreads/gr-book-link.parquet", Default::default())?;

    let data = data.join(links, &[col("book_id")], &[col("book_id")], JoinType::Inner);
    Ok(data)
  }

  /// Filter the data frame to only the actions we want
  fn filter(&self, frame: LazyFrame) -> LazyFrame {
    match self.actions {
      ActionType::Ratings => {
        frame.filter(col("rating").is_not_null())
      },
      _ => frame
    }
  }

  /// Create an identity column reference.
  fn id_col(&self) -> Expr {
    match self.clusters {
      AggType::Clusters => {
        info!("grouping by integrated clusters");
        col("cluster")
      },
      AggType::NativeWorks => {
        info!("grouping by native works");
        when(
          col("work_id").is_not_null()
        ).then(
          col("work_id") + lit(NS_GR_WORK.base())
        ).otherwise(
          col("book_id") + lit(NS_GR_BOOK.base())
        )
      },
    }
  }

  /// Project and sort (if possible) the data.
  fn project_and_sort(&self, frame: LazyFrame) -> LazyFrame {
    match self.data {
      SrcType::Simple => {
        frame.select(&[
          col("user_id").alias("user"),
          self.id_col().alias("item"),
          col("rating"),
        ])
      },
      SrcType::Full => {
        frame.select(&[
          col("user_id").alias("user"),
          self.id_col().alias("item"),
          (col("updated").cast(DataType::Int64) / lit(1000)).alias("timestamp"),
          col("rating"),
        ])
      }
    }
  }

  /// Aggreate the interactions.
  fn aggregates(&self) -> Vec<Expr> {
    match (&self.actions, &self.data) {
      (ActionType::Ratings, SrcType::Simple) => {
        vec![
          col("rating").median().alias("rating"),
          col("item").count().alias("nratings"),
        ]
      },
      (ActionType::Ratings, SrcType::Full) => {
        vec![
          col("rating").median().alias("rating"),
          col("rating").last().alias("last_rating"),
          col("timestamp").min().alias("first_time"),
          col("timestamp").max().alias("last_time"),
          col("item").count().alias("nratings"),
        ]
      },
      (ActionType::AddActions, SrcType::Simple) => {
        vec![
          col("item").count().alias("nactions"),
        ]
      },
      (ActionType::AddActions, SrcType::Full) => {
        vec![
          col("timestamp").min().alias("first_time"),
          col("timestamp").max().alias("last_time"),
          col("item").count().alias("nactions"),
        ]
      },
    }
  }

  fn maybe_integrate_ratings(&self, actions: LazyFrame, source: &LazyFrame) -> LazyFrame {
    match &self.actions {
      ActionType::AddActions => {
        let ratings = source.clone().filter(col("rating").is_not_null());
        let ratings = ratings.groupby(["user", "item"]).agg(&[
          col("rating").last().alias("last_rating")
        ]);
        actions.join(ratings, &[col("user"), col("item")], &[col("user"), col("item")], JoinType::Left)
      },
      _ => actions
    }
  }
}
