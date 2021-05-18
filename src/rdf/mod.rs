//! Module for working with RDF-formatted data.
//!
//! We implement this ourselves to get a small subset, rather than pulling in all
//! the dependenices and complexity of Oxigraph.

pub mod model;
pub mod parse;

pub use model::Triple;
