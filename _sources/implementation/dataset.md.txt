---
title: Dataset Design
---

# Design for Datasets

[Polars]: https://pola-rs.github.io/polars-book/

The general import philosophy is that we scan raw data from underlying data sets
into a tabular form, and then integrate it with further code; import and
processing stages are written in Rust, using the [Polars][] library for data
frames.  We use Parquet for storing all outputs, both intermediate stages and
final products; when an output is particularly small, and a CSV version would be
convenient, we sometimes also produce compressed CSV.

## Adding a Data Set

In general, to add new data, you need to do a few things:

1.  Add the source files under `data`, and commit them to DVC.
2.  Implement code to extract the source files into tabular Parquet that keeps
    identifiers, etc. from the original source, but is easier to process for
    subsequent stages.  This typically includes a new Rust command to process
    the data, and a DVC stage to run it.
3.  If the data source provides additional ISBNs, add them to
    `book-links/all-isbns.toml` so that they are included in ISBN indexing.
4.  Implement code to process the extracted source files into cluster-aggregated
    files, if needed (typically used for rating data).
5.  Update the analytics and statistics to include the new data.

All of the CLI tools live in {rust:mod}`bookdata::cli`, with support code
elsewhere in the source tree.
