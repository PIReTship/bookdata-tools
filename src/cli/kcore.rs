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

        let mut iters = 0;
        // we proceed iteratively, alternating filtering users and items
        loop {
            let nstart = actions.height();
            info!("pass {}: checking items of {} actions", iters + 1, nstart);
            let ics = actions.column("item")?.value_counts(true, false)?;
            let ic_min: u32 = ics
                .column("counts")?
                .min()
                .ok_or_else(|| anyhow!("data frame is empty"))?;
            if ic_min < ik {
                info!("filtering items (smallest count: {})", ic_min);
                let ifilt = ics
                    .lazy()
                    .filter(col("counts").gt_eq(lit(ik)))
                    .select(&[col("item")]);
                let afilt = actions.lazy().inner_join(ifilt, "item", "item");
                actions = afilt.collect()?;
                info!(
                    "now have {} actions (removed {})",
                    actions.height(),
                    nstart - actions.height()
                );
            }

            let nustart = actions.height();
            info!("pass {}: checking users of {} actions", iters + 1, nustart);
            let ucs = actions.column("user")?.value_counts(true, false)?;
            let uc_min: u32 = ucs
                .column("counts")?
                .min()
                .ok_or_else(|| anyhow!("data frame is empty"))?;
            if uc_min < uk {
                info!("filtering users (smallest count: {})", uc_min);
                let ufilt = ucs
                    .lazy()
                    .filter(col("counts").gt_eq(lit(uk)))
                    .select(&[col("user")]);
                let afilt = actions.lazy().inner_join(ufilt, "user", "user");
                actions = afilt.collect()?;
                info!(
                    "now have {} actions (removed {})",
                    actions.height(),
                    nustart - actions.height()
                );
            } else {
                info!(
                    "finished computing {}-core with {} actions (imin: {}, umin: {})",
                    self.k,
                    nustart,
                    // re-compute this in case it changed
                    actions
                        .column("item")?
                        .value_counts(true, false)?
                        .column("counts")?
                        .min::<u32>()
                        .unwrap(),
                    uc_min
                );
                break;
            }

            iters += 1;
        }

        save_df_parquet(actions, &self.output)?;

        Ok(())
    }
}
