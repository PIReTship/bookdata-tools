//! Code for processing and integrating book data.
//!
//! The `bookdata` crate (not published) provides the support and utility
//! code for the various programs used to integrate the book data.  If you
//! are writing additional integrations or analyses, you may find the
//! modules and functions in here useful.
pub mod cleaning;
pub mod parsing;
pub mod tsv;
pub mod util;
pub mod db;
pub mod io;
pub mod ids;
pub mod graph;
pub mod marc;
pub mod tracking;
pub mod arrow;
pub mod commands;
pub mod cli;
pub mod prelude;

pub use ids::index;
