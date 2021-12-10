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
[Rust API documentation](../apidocs/bookdata/) for further details.

The `bd-macros` directory contains the `TableRow` derive macro, because procedural macros cannot
live in the same crate in which they are used.  Most users won't need to adjust this macro.

## DataFusion SQL

[DF]: https://github.com/apache/arrow-datafusion/
[Molt]: https://wduquette.github.io/molt/

We do some processing with [DataFusion SQL][DF].  We have used [Molt][] (a Rust-based variant of
TCL) to implement a small language for setting up tables, running queries, and saving the results.
The `fusion` bookdata command runs scripts in this language.

The following is a short example script demonstrating a join between two tables:

```tcl
table isbns "../book-links/all-isbns.parquet"
table books "book-isbns.parquet"

save-results "book-isbn-ids.parquet" {
    SELECT rec_id, isbn_id
    FROM books JOIN isbns USING (isbn)
}
```

The `save-results` command can write to either CSV or Parquet.
