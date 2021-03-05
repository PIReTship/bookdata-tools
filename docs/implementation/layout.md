---
title: Layout
parent: Implementation
nav_order: 2
---

# Layout
{: .no_toc}

The import code consists of Python, Rust, and SQL code, wired together with DVC.

## Python Scripts

Python scripts live under `scripts`, as a Python package.  They should not be launched directly, but
rather via `run.py`, which will make sure the environment is set up properly for them:

    python run.py sql-script [options] script.sql

## SQL Scripts

Our SQL scripts are run with a custom SQL script runner (the `sql-script` Python script), that breaks
them into chunks, handles errors, and tracks dependencies and script status.  The script runner parses
directives in SQL comments; for example:

    --- #step ISBN ID storage
    CREATE TABLE IF NOT EXISTS isbn_id (
    isbn_id SERIAL PRIMARY KEY,
    isbn VARCHAR NOT NULL UNIQUE
    );

is a step called "ISBN ID storage".  Each step is processed in a transaction that is committed at the
end, so steps are atomic (unless marked with `#notx`).

These are the directives for steps:

- `#step LABEL` starts a new step with the label `LABEL`.  Additional directives before the first
  SQL statement will apply to this step.
- `#notx` means the step will run in autocommit mode.  This is needed for certain maintenance commands
  that do not work within transactions.
- `#allow CODE` allows the PostgreSQL error 'code', such as `invalid_table_definition`.  The script
  will not fail if the step fails with this error.  Used for dealing with steps that do things like
  create indexes, so if the index already exists it is fine to still run the script.

In addition, the top of the file can have `#dep` directives, that indicate the dependencies of this
script.  The only purpose of the `#dep` is to record dependencies in the database stage state
table, so that modifications can propagate and be detected; dependencies still need to be recorded
in `.dvc` files to run the import steps in the correct order.

## Utility Code

The `bookdata` package contains Python utility code, and the `src` directory contains a number
of utility modules for use in the Rust code.  To the extent reasonable, we have tried to mirror
design patterns and function names.
