use std::fs::File;

use clap::Args;

use crate::prelude::*;
use ::polars::prelude::*;

/// Compute k-cores of interaction records.
#[derive(Debug, Clone, Args)]
#[command(name = "kcore")]
pub struct Kcore {
    /// The size of the k-core.
    #[arg(short = 'k', long = "k", default_value_t = 5)]
    k: u32,

    /// The output file.
    #[arg(short = 'o', long = "output", name = "FILE")]
    output: PathBuf,

    /// The input file
    #[arg(name = "INPUT")]
    input: PathBuf,
}

impl Command for Kcore {
    fn exec(&self) -> Result<()> {
        info!("computing {}-core for {}", self.k, self.input.display());

        let file = File::open(&self.input)?;
        let mut actions = ParquetReader::new(file).finish()?;
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
            if ic_min < self.k {
                info!("filtering items (smallest count: {})", ic_min);
                let ifilt = ics
                    .lazy()
                    .filter(col("counts").gt_eq(lit(self.k)))
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
            if uc_min < self.k {
                info!("filtering users (smallest count: {})", uc_min);
                let ufilt = ucs
                    .lazy()
                    .filter(col("counts").gt_eq(lit(self.k)))
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
