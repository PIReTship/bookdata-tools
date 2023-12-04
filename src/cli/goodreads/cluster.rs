use std::path::{Path, PathBuf};

use clap::Args;
use parse_display::Display;

use crate::arrow::*;
use crate::ids::codes::{NS_GR_BOOK, NS_GR_WORK};
use crate::prelude::*;

use polars::prelude::*;

#[derive(Args, Debug)]
pub struct CICommand {
    /// Cluster ratings
    #[arg(long = "ratings")]
    ratings: bool,

    /// Cluster add-to-shelf actions
    #[arg(long = "add-actions")]
    add_actions: bool,

    /// Cluster reviews actions
    #[arg(long = "reviews")]
    reviews: bool,

    /// Cluster using simple data instead of full data.
    #[arg(long = "simple")]
    simple: bool,

    /// Cluster using native GoodReads works instead of book clusters.
    #[arg(long = "native-works")]
    native_works: bool,

    /// Write output to FILE
    #[arg(short = 'o', long = "output", name = "FILE")]
    output: PathBuf,
}

#[derive(Debug, PartialEq, Eq, Display)]
#[display(style = "kebab-case")]
enum SrcType {
    Simple,
    Full,
}

#[derive(Debug, PartialEq, Eq)]
enum ActionType {
    Ratings,
    AddActions,
    Reviews,
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

impl CICommand {
    pub fn exec(&self) -> Result<()> {
        let mut op = if self.add_actions {
            ClusterOp::add_actions(&self.output)
        } else if self.ratings {
            ClusterOp::ratings(&self.output)
        } else if self.reviews {
            ClusterOp::reviews(&self.output)
        } else {
            error!("must specify one of --add-actions, --ratings, or --reviews");
            return Err(anyhow!("no operating mode specified"));
        };
        if self.native_works {
            op = op.native_works();
        }
        if self.simple {
            op = op.simple();
        }

        op.cluster()
    }
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

    /// Start a new review-clustering operation.
    pub fn reviews<P: AsRef<Path>>(path: P) -> ClusterOp {
        ClusterOp {
            actions: ActionType::Reviews,
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
        let interactions = match self.actions {
            ActionType::Reviews => self.load_reviews()?,
            ActionType::AddActions | ActionType::Ratings => self.load_interactions()?,
        };
        let interactions = self.filter(interactions);
        let interactions = self.project_and_sort(interactions);
        let actions = interactions
            .clone()
            .group_by(&[col("user"), col("item")])
            .agg(self.aggregates());

        let actions = self.maybe_integrate_ratings(actions, &interactions);

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
        let dir = match self.data {
            SrcType::Full => "full",
            SrcType::Simple => "simple",
        };
        let path = format!("goodreads/{}/gr-interactions.parquet", dir);
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

    /// Load the review file.
    fn load_reviews(&self) -> Result<LazyFrame> {
        let dir = match self.data {
            SrcType::Full => "full",
            SrcType::Simple => {
                error!("only full data has reviews");
                return Err(anyhow!("invalid combination of options"));
            }
        };
        let path = format!("goodreads/{}/gr-reviews.parquet", dir);
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
        match self.data {
            SrcType::Simple => frame.select(&[
                col("user_id").alias("user"),
                self.id_col().alias("item"),
                col("rating"),
            ]),
            SrcType::Full => frame.select(&[
                col("user_id").alias("user"),
                self.id_col().alias("item"),
                (col("updated").cast(DataType::Int64)).alias("timestamp"),
                col("rating"),
            ]),
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
            }
            (ActionType::Ratings, SrcType::Full) => {
                vec![
                    col("rating").median().alias("rating"),
                    col("rating").last().alias("last_rating"),
                    col("timestamp").min().alias("first_time"),
                    col("timestamp").max().alias("last_time"),
                    col("item").count().alias("nratings"),
                ]
            }
            (ActionType::AddActions, SrcType::Simple) => {
                vec![col("item").count().alias("nactions")]
            }
            (ActionType::AddActions, SrcType::Full) => {
                vec![
                    col("timestamp").min().alias("first_time"),
                    col("timestamp").max().alias("last_time"),
                    col("item").count().alias("nactions"),
                ]
            }
            (ActionType::Reviews, _) => unreachable!(),
        }
    }

    fn maybe_integrate_ratings(&self, actions: LazyFrame, source: &LazyFrame) -> LazyFrame {
        match &self.actions {
            ActionType::AddActions => {
                let ratings = source.clone().filter(col("rating").is_not_null());
                let ratings = ratings
                    .group_by(["user", "item"])
                    .agg(&[col("rating").last().alias("last_rating")]);
                actions.join(
                    ratings,
                    &[col("user"), col("item")],
                    &[col("user"), col("item")],
                    JoinType::Left.into(),
                )
            }
            _ => actions,
        }
    }
}
