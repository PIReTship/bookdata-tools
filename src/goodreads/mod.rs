//! Schemas and processing logic for GoodReads data.
//!
//! This module contains the code for processing GoodReads data from the UCSD Book Graph.
//! The data layout is documented at <https://bookdata.piret.info/data/goodreads.html>.
pub mod author;
pub mod book;
pub mod genres;
pub mod interaction;
pub mod review;
pub mod simple_interaction;
pub mod work;
