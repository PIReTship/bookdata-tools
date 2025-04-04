use std::path::{Path, PathBuf};

use clap::Args;

use crate::arrow::*;
use crate::ids::codes::{NS_GR_BOOK, NS_GR_WORK};
use crate::prelude::*;

use polars::prelude::*;

#[derive(Args, Debug)]
pub struct CICommand {
    /// Cluster ratings.
    #[arg(long = "ratings")]
    ratings: bool,

    /// Cluster add-to-shelf actions.
    #[arg(long = "add-actions")]
    add_actions: bool,

    /// Cluster using native GoodReads works instead of book clusters.
    #[arg(long = "native-works")]
    native_works: bool,

    /// Write output to FILE.
    #[arg(short = 'o', long = "output", name = "FILE")]
    output: PathBuf,
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
    clusters: AggType,
    output: PathBuf,
}

impl CICommand {
    pub fn exec(&self) -> Result<()> {
        let mut op = if self.add_actions {
            ClusterOp::add_actions(&self.output)
        } else if self.ratings {
            ClusterOp::ratings(&self.output)
        } else {
            error!("must specify one of --add-actions, --ratings, or --reviews");
            return Err(anyhow!("no operating mode specified"));
        };
        if self.native_works {
            op = op.native_works();
        }

        op.cluster()
    }
}

impl ClusterOp {
    /// Start a new action-clustering operation.
    pub fn add_actions<P: AsRef<Path>>(path: P) -> ClusterOp {
        ClusterOp {
            actions: ActionType::AddActions,
            clusters: AggType::Clusters,
            output: path.as_ref().to_path_buf(),
        }
    }

    /// Start a new rating-clustering operation.
    pub fn ratings<P: AsRef<Path>>(path: P) -> ClusterOp {
        ClusterOp {
            actions: ActionType::Ratings,
            clusters: AggType::Clusters,
            output: path.as_ref().to_path_buf(),
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
        let interactions = self.load_interactions()?;
        let interactions = self.filter(interactions);
        let interactions = self.project_and_sort(interactions);
        let actions = interactions
            .clone()
            .group_by(&[col("user_id"), col("item_id")])
            .agg(self.aggregates());

        let actions = self.maybe_integrate_ratings(actions, &interactions);
        let actions = actions.sort("first_time", SortOptions::default());

        debug!("logical plan: {:?}", actions.describe_plan());
        debug!("optimized plan: {:?}", actions.describe_optimized_plan()?);
        info!("collecting results");
        let actions = actions.collect()?;

        info!("writing {} actions to {:?}", actions.height(), &self.output);
        save_df_parquet(actions, &self.output)?;

        Ok(())
    }

    /// Load the interaction file.
    fn load_interactions(&self) -> Result<LazyFrame> {
        let path = "goodreads/gr-interactions.parquet";
        let data = LazyFrame::scan_parquet(path, Default::default())?;

        let links = LazyFrame::scan_parquet("goodreads/gr-book-link.parquet", Default::default())?;

        let data = data.join(
            links,
            &[col("book_id")],
            &[col("book_id")],
            JoinType::Inner.into(),
        );
        Ok(data)
    }

    /// Filter the data frame to only the actions we want
    fn filter(&self, frame: LazyFrame) -> LazyFrame {
        match self.actions {
            ActionType::Ratings => frame.filter(col("rating").is_not_null()),
            _ => frame,
        }
    }

    /// Create an identity column reference.
    fn id_col(&self) -> Expr {
        match self.clusters {
            AggType::Clusters => {
                info!("grouping by integrated clusters");
                col("cluster")
            }
            AggType::NativeWorks => {
                info!("grouping by native works");
                when(col("work_id").is_not_null())
                    .then(col("work_id") + lit(NS_GR_WORK.base()))
                    .otherwise(col("book_id") + lit(NS_GR_BOOK.base()))
            }
        }
    }

    /// Project and sort (if possible) the data.
    fn project_and_sort(&self, frame: LazyFrame) -> LazyFrame {
        frame.select(&[
            col("user_id"),
            self.id_col().alias("item_id"),
            (col("updated").cast(DataType::Int64)).alias("timestamp"),
            col("rating"),
        ])
    }

    /// Aggreate the interactions.
    fn aggregates(&self) -> Vec<Expr> {
        match &self.actions {
            ActionType::Ratings => {
                vec![
                    col("rating").median().alias("rating"),
                    col("rating").last().alias("last_rating"),
                    col("timestamp").min().alias("first_time"),
                    col("timestamp").max().alias("last_time"),
                    col("item_id").count().alias("nratings"),
                ]
            }
            ActionType::AddActions => {
                vec![
                    col("timestamp").min().alias("first_time"),
                    col("timestamp").max().alias("last_time"),
                    col("item_id").count().alias("nactions"),
                ]
            }
        }
    }

    fn maybe_integrate_ratings(&self, actions: LazyFrame, source: &LazyFrame) -> LazyFrame {
        match &self.actions {
            ActionType::AddActions => {
                let ratings = source.clone().filter(col("rating").is_not_null());
                let ratings = ratings
                    .group_by(["user_id", "item_id"])
                    .agg(&[col("rating").last().alias("last_rating")]);
                actions.join(
                    ratings,
                    &[col("user_id"), col("item_id")],
                    &[col("user_id"), col("item_id")],
                    JoinType::Left.into(),
                )
            }
            _ => actions,
        }
    }
}
