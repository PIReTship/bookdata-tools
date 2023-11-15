//! Collect ISBNs from across the data sources.
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::read_to_string;

use serde::Deserialize;
use toml;

use crate::prelude::Result;
use crate::prelude::*;
use polars::prelude::*;

/// Collect ISBNs from across the data sources.
#[derive(Args, Debug)]
#[command(name = "collect-isbns")]
pub struct CollectISBNs {
    /// Path to the output file (in Parquet format)
    #[arg(short = 'o', long = "output")]
    out_file: PathBuf,

    /// path to the ISBN source definition file (in TOML format)
    #[arg(name = "DEFS")]
    source_file: PathBuf,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum MultiSource {
    Single(ISBNSource),
    Multi(Vec<ISBNSource>),
}

#[derive(Deserialize, Debug)]
struct ISBNSource {
    path: String,
    #[serde(default)]
    column: Option<String>,
}

/// The type of ISBN source set specifications.
type SourceSet = HashMap<String, MultiSource>;

impl MultiSource {
    fn to_list(&self) -> Vec<&ISBNSource> {
        match self {
            MultiSource::Single(s) => vec![&s],
            MultiSource::Multi(ss) => ss.iter().map(|s| s).collect(),
        }
    }
}

/// Read a single ISBN source into the accumulator.
fn scan_source(name: &str, src: &ISBNSource) -> Result<LazyFrame> {
    let id_col = src.column.as_deref().unwrap_or("isbn");

    info!("scanning ISBNs from {} (column {})", src.path, id_col);

    let df = if src.path.ends_with(".csv") {
        LazyCsvReader::new(src.path.to_string())
            .has_header(true)
            .finish()?
    } else {
        scan_df_parquet(&src.path)?
    };
    let df = df.select(&[col(id_col).alias("isbn")]);
    let df = df.drop_nulls(None);
    let df = df.group_by(["isbn"]).agg([count().alias(name)]);

    Ok(df)
}

impl Command for CollectISBNs {
    fn exec(&self) -> Result<()> {
        info!("reading spec from {}", self.source_file.display());
        let spec = read_to_string(&self.source_file)?;
        let spec: SourceSet = toml::de::from_str(&spec)?;
        let ndfs = spec.len();

        let mut df: Option<LazyFrame> = None;
        let mut columns = vec!["isbn"];
        for (name, ms) in &spec {
            let mut n = 0;
            for source in ms.to_list() {
                let sdf = scan_source(name, source)?;
                if let Some(cur) = df {
                    let mut jdf =
                        cur.join(sdf, &[col("isbn")], &[col("isbn")], JoinType::Outer.into());
                    if n > 0 {
                        let mut cols: Vec<Expr> = columns.iter().map(|n| col(n)).collect();
                        cols.push(
                            (col(name).fill_null(0) + col(&format!("{}_right", name)).fill_null(0))
                                .alias(name),
                        );
                        jdf = jdf.select(&cols);
                    }
                    df = Some(jdf);
                } else {
                    df = Some(sdf);
                }
                n += 1;
            }
            columns.push(name);
        }
        let df = df.ok_or_else(|| anyhow!("no source files loaded"))?;
        let df = df.fill_null(lit(0));
        let df = df.with_row_count("isbn_id", Some(1));
        let mut cast = vec![col("isbn_id").cast(DataType::Int32)];
        cast.extend(columns.iter().map(|c| col(c)));
        let df = df.select(&cast);
        info!("collecting ISBNs from {} sources", ndfs);
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
