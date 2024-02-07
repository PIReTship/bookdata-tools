use std::fs::File;

use chrono::NaiveDate;
use clap::Args;

use crate::prelude::*;
use polars::prelude::*;

/// Compute k-cores of interaction records.
#[derive(Debug, Clone, Args)]
#[command(name = "kcore")]
pub struct Kcore {
    /// The size of the k-core.
    #[arg(short = 'k', long = "k", default_value_t = 5)]
    k: u32,

    /// The user rating count for a (ku,ki)-core
    #[arg(short = 'U', long = "user-k")]
    user_k: Option<u32>,

    /// The item rating count for a (ku,ki)-core
    #[arg(short = 'I', long = "item-k")]
    item_k: Option<u32>,

    /// Limit to ratings in a particular year.
    #[arg(long = "year")]
    year: Option<i32>,

    /// Limit ratings to after a particular date (inclusive)
    #[arg(long = "start-date")]
    start: Option<NaiveDate>,

    /// Limit ratings to before a particular date (exclusive)
    #[arg(long = "end-date")]
    end: Option<NaiveDate>,

    /// The output file.
    #[arg(short = 'o', long = "output", name = "FILE")]
    output: PathBuf,

    /// The input file
    #[arg(name = "INPUT")]
    input: PathBuf,
}

impl Command for Kcore {
    fn exec(&self) -> Result<()> {
        let uk = self.user_k.unwrap_or(self.k);
        let ik = self.item_k.unwrap_or(self.k);
        info!(
            "computing ({},{})-core for {}",
            uk,
            ik,
            self.input.display()
        );

        let file = File::open(&self.input)?;
        let mut actions = ParquetReader::new(file).finish()?;

        let start = self
            .start
            .or_else(|| self.year.map(|y| NaiveDate::from_ymd_opt(y, 1, 1).unwrap()));
        let end = self.end.or_else(|| {
            self.year
                .map(|y| NaiveDate::from_ymd_opt(y + 1, 1, 1).unwrap())
        });

        if let Some(start) = start {
            info!("removing actions before {}", start);
            let start = start.and_hms_opt(0, 0, 0).unwrap().timestamp();
            // currently hard-coded for goodreads
            let col = actions.column("last_time")?;
            let mask = col.gt_eq(start)?;
            actions = actions.filter(&mask)?;
        }
        if let Some(end) = end {
            info!("removing actions after {}", end);
            let end = end.and_hms_opt(0, 0, 0).unwrap().timestamp();
            // currently hard-coded for goodreads
            let col = actions.column("last_time")?;
            let mask = col.lt(end)?;
            actions = actions.filter(&mask)?;
        }

        let n_initial = actions.height();
        let mut n_last = 0;
        let mut iters = 0;
        // we proceed iteratively, alternating filtering users and items
        // stop when a pass has left it unchanged
        while actions.height() != n_last {
            n_last = actions.height();
            info!(
                "pass {}: checking items of {} actions",
                iters + 1,
                friendly::scalar(actions.height())
            );
            actions = filter_counts(actions, "item", ik)?;

            info!(
                "pass {}: checking users of {} actions",
                iters + 1,
                friendly::scalar(actions.height())
            );
            actions = filter_counts(actions, "user", ik)?;

            iters += 1;
        }
        info!(
            "finished computing {}-core with {} of {} actions (imin: {}, umin: {})",
            self.k,
            friendly::scalar(actions.height()),
            friendly::scalar(n_initial),
            // re-compute this in case it changed
            actions
                .column("item")?
                .value_counts(true, true)?
                .column("counts")?
                .min::<u32>()?
                .unwrap(),
            actions
                .column("user")?
                .value_counts(true, true)?
                .column("counts")?
                .min::<u32>()?
                .unwrap(),
        );

        save_df_parquet(actions, &self.output)?;

        Ok(())
    }
}

fn filter_counts(actions: DataFrame, column: &'static str, k: u32) -> Result<DataFrame> {
    let nstart = actions.height();
    let counts = actions.column(column)?.value_counts(true, true)?;
    let min_count: u32 = counts
        .column("counts")?
        .min()?
        .ok_or_else(|| anyhow!("data frame is empty"))?;
    if min_count < k {
        info!("filtering {}s (smallest count: {})", column, min_count);
        let ifilt = counts
            .lazy()
            .filter(col("counts").gt_eq(lit(k)))
            .select(&[col(column)]);
        let afilt = actions.lazy().inner_join(ifilt, column, column);
        let actions = afilt.collect()?;
        info!(
            "now have {} actions (removed {})",
            friendly::scalar(actions.height()),
            nstart - actions.height()
        );
        Ok(actions)
    } else {
        Ok(actions)
    }
}
