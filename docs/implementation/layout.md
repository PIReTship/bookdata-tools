# Code Layout

The import code consists primarily of Rust, wired together with DVC, with data
in several directories to facilitate ease of discovery.  We use Python and R
in Quarto documents for analytics and reporting.

## Rust

The Rust code all lives under `src`, with the various command-line programs in `src/cli`.  The Rust
tools are implemented as a monolithic executable with subcommands for various operations, to save
disk space and compile time.  To see the help:

    cargo run help

The programs are run through `cargo run` in `--release` mode; the
[`bd.cmd`](pipeline.md#bd.cmd) jsonnet function automates this, so we only need
to specify the subcommand and its options in our pipeline definitions.

For writing new commands, there is a lot of utility code under `src`.  Consult
the [Rust API documentation](/apidocs/bookdata/) for further details.

The Rust code makes extensive use of the [polars][], [arrow2][], and
[parquet2][] crates for data analysis and IO.  [arrow2_convert][] is used to
automate converstion for Parquet serialization.

[polars]: https://docs.rs/polars
[arrow2]: https://docs.rs/arrow2
[arrow2_convert]: https://docs.rs/arrow2_convert
[parquet2]: https://docs.rs/parquet2
