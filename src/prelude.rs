pub use crate::arrow::reader::scan_df_parquet;
pub use crate::arrow::writer::save_df_parquet;
pub use crate::arrow::TableRow;
pub use crate::cli::Command;
pub use crate::io::ext::LengthRead;
pub use crate::io::file_size;
pub use crate::io::path_list;
pub use crate::io::DataSink;
pub use crate::io::LineProcessor;
pub use crate::io::ObjectWriter;
pub use crate::layout::*;
pub use crate::util::default;
pub use crate::util::Timer;
pub use anyhow::{anyhow, Result};
pub use clap::Args;
pub use fallible_iterator::FallibleIterator;
pub use log::*;
pub use std::borrow::Cow;
pub use std::path::{Path, PathBuf};

/// Macro to implement FromStr using JSON.
#[macro_export]
macro_rules! json_from_str {
    ($name:ident) => {
        impl FromStr for $name {
            type Err = serde_json::Error;

            fn from_str(s: &str) -> serde_json::Result<$name> {
                return serde_json::from_str(s);
            }
        }
    };
}
