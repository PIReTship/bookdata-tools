---
title: Code Layout
---

# Code Layout

The import code consists of Python, Rust, and some SQL code, wired together with DVC, with data
in several directories to facilitate ease of discovery.

## Python Scripts

Python scripts live in the various directories in which they operate. They should not be launched
directly, but rather via `run.py`, which will make sure the environment is set up properly for them:

    python run.py my-script-file.py

The `bookdata` package contains a little Python utility code.  The `run.py` script ensures it is
available in the Python import path.

## Rust

The Rust code all lives under `src`, with the various command-line programs in `src/cli`.  The Rust
tools are implemented as a monolithic executable with subcommands for various operations, to save
disk space and compile time.  To see the help:

    cargo run help

Or through Python:

    python run.py --rust help

The `run.py` script with the `--rust` option sets up some environment variables to ensure that
the Rust code builds correctly inside a Conda environment, and also defaults to using a release
build (`cargo run` uses debug builds by default).  All DVC pipeline stages use `run.py` to run
the Rust tools.

For writing new commands, there is a lot of utility code under `src`.  Consult the
{rust:mod}`Rust API documentation <bookdata>` for further details.

The Rust code makes extensive use of the [polars][], [arrow2][], and
[parquet2][] crates for data analysis and IO.  [arrow2_convert][] is used to
automate converstion for Parquet serialization.

[polars]: https://docs.rs/polars
[arrow2]: https://docs.rs/arrow2
[arrow2_convert]: https://docs.rs/arrow2_convert
[parquet2]: https://docs.rs/parquet2
