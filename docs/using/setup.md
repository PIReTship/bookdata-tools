---
title: Setup
parent: Importing
nav_order: 2
---

# Setting Up the Environment

These tools require an Anaconda installation.  It is possible to use them without Anaconda, but we have provided
the environment definitions to automate use with Anaconda.

## System Requirements

You will need:

- Anaconda or Miniconda
- 100GB of disk space for input files
- Several GB of memory (lower limit not know; the code is known to run in 64GB of RAM; 16GB may be feasible)

## Import Tool Dependencies

The import tools are written in Python and Rust.  The provided `environment.yml` file defines an
Anaconda environment (named `bookdata` by default) that contains all required runtimes and
libraries:

    conda env create -f environment.yml
    conda activate bookdata

If you don't want to use Anaconda, see the following for more details on dependencies.

### Python

This needs the following Python dependencies:

- Python 3.8 or later
- numpy
- pandas
- seaborn
- dvc (2.0 or later)

### Rust

The Rust tools need Rust version 1.55 or later.  The easiest way to install this — besides Anaconda — is with
[rustup](https://www.rust-lang.org/learn/get-started).

The `cargo` build tool will automatically download all Rust libraries required.  The Rust code does not depend on any specific system libraries.
