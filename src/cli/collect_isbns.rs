//! Collect ISBNs from across the data sources.
use std::fmt::Debug;

use fallible_iterator::IteratorExt;
use polars::prelude::*;

use crate::prelude::Result;
use crate::prelude::*;

/// Collect ISBNs from across the data sources.
#[derive(Args, Debug)]
#[command(name = "collect-isbns")]
pub struct CollectISBNs {
    /// Path to the output file (in Parquet format)
    #[arg(short = 'o', long = "output")]
    out_file: PathBuf,
}

/// Get the active ISBN layouts.
///
/// Modify this function to add more sources.
fn all_sources(cfg: &Config) -> Vec<ISBNSource> {
    vec![
        ISBNSource::new("LOC")
            .path("../loc-mds/book-isbns.parquet")
            .finish(),
        ISBNSource::new("OL")
            .path("../openlibrary/edition-isbns.parquet")
            .finish(),
        ISBNSource::new("GR")
            .enabled(cfg.goodreads.enabled)
            .path("../goodreads/gr-book-ids.parquet")
            .columns(&["isbn10", "isbn13", "asin"])
            .finish(),
        ISBNSource::new("BX")
            .enabled(cfg.bx.enabled)
            .path("../bx/cleaned-ratings.csv")
            .finish(),
        ISBNSource::new("AZ14")
            .enabled(cfg.az2014.enabled)
            .path("../az2014/ratings.parquet")
            .column("asin")
            .finish(),
        ISBNSource::new("AZ18")
            .enabled(cfg.az2018.enabled)
            .path("../az2018/ratings.parquet")
            .column("asin")
            .finish(),
    ]
}

#[derive(Debug, Clone)]
struct ISBNSource {
    name: &'static str,
    enabled: bool,
    path: &'static str,
    columns: Vec<&'static str>,
}

impl ISBNSource {
    fn new(name: &'static str) -> ISBNSource {
        ISBNSource {
            name: name,
            enabled: true,
            path: "",
            columns: vec![],
        }
    }

    fn enabled(self, e: bool) -> ISBNSource {
        ISBNSource { enabled: e, ..self }
    }

    fn path(self, path: &'static str) -> ISBNSource {
        ISBNSource { path, ..self }
    }

    fn column(self, col: &'static str) -> ISBNSource {
        ISBNSource {
            columns: vec![col],
            ..self
        }
    }

    fn columns(self, cols: &[&'static str]) -> ISBNSource {
        ISBNSource {
            columns: cols.iter().map(|s| *s).collect(),
            ..self
        }
    }

    fn finish(self) -> ISBNSource {
        ISBNSource {
            columns: if self.columns.len() > 0 {
                self.columns
            } else {
                vec!["isbn".into()]
            },
            ..self
        }
    }
}

/// Read a single ISBN source into the accumulator.
fn scan_source(src: &ISBNSource) -> Result<LazyFrame> {
    info!("scanning ISBNs from {}", src.path);

    let read = if src.path.ends_with(".csv") {
        LazyCsvReader::new(src.path.to_string())
            .has_header(true)
            .finish()?
    } else {
        scan_df_parquet(src.path)?
    };

    let mut counted: Option<LazyFrame> = None;
    for id_col in &src.columns {
        info!("counting column {}", id_col);
        let df = read.clone().select(&[col(id_col).alias("isbn")]);
        let df = df.drop_nulls(None);
        let df = df.group_by(["isbn"]).agg([count().alias("nrecs")]);
        if let Some(prev) = counted {
            let joined = prev.join(df, &[col("isbn")], &[col("isbn")], JoinType::Outer.into());
            counted = Some(joined.select([
                col("isbn"),
                (col(src.name).fill_null(0) + col("nrecs").fill_null(0)).alias(src.name),
            ]));
        } else {
            counted = Some(df.select([col("isbn"), col("nrecs").alias(src.name)]));
        }
    }

    Ok(counted.expect("data frame with no columns"))
}

impl Command for CollectISBNs {
    fn exec(&self) -> Result<()> {
        let cfg = load_config()?;
        let sources = all_sources(&cfg);
        let active: Vec<_> = sources.iter().filter(|s| s.enabled).collect();
        info!(
            "collecting ISBNs from {} active sources (of {} known)",
            active.len(),
            sources.len()
        );

        let df = active
            .iter()
            .map(|s| scan_source(*s))
            .transpose_into_fallible()
            .fold(None, |cur, df2| {
                Ok(cur
                    .map(|df1: LazyFrame| {
                        df1.join(
                            df2.clone(),
                            &[col("isbn")],
                            &[col("isbn")],
                            JoinType::Outer.into(),
                        )
                    })
                    .or(Some(df2)))
            })?;

        let df = df.ok_or_else(|| anyhow!("no sources loaded"))?;
        let df = df.with_row_count("isbn_id", Some(1));
        let mut cast = vec![col("isbn_id").cast(DataType::Int32), col("isbn")];
        for src in &active {
            cast.push(col(src.name).fill_null(0));
        }
        let df = df.select(&cast);
        info!("collecting ISBNs");
        let df = df.collect()?;

        info!(
            "saving {} ISBNs to {}",
            df.height(),
            self.out_file.display()
        );
        save_df_parquet(df, &self.out_file)?;
        info!("wrote ISBN collection file");

        Ok(())
    }
}
