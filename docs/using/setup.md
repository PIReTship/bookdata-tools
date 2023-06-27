---
title: Setup
---

# Setting Up the Environment

These tools require an Anaconda installation.  It is possible to use them without Anaconda, but we have provided
the environment definitions to automate use with Anaconda.

This project uses Git submodules, so you should clone it with:

    git clone --recursive https://github.com/PIReTship/bookdata-tools.git

## System Requirements

You will need:

- Anaconda or Miniconda
- 250GB of disk space
- At least 24 GB of memory (lower may be possible)

## Import Tool Dependencies

The import tools are written in Python and Rust.  The provided Conda lockfiles,
along with `environment.yml`, provide the data to define an Anaconda environment
that contains all required runtimes and libraries:

    conda-lock install -n bookdata
    conda activate bookdata

If you don't want to use Anaconda, see the following for more details on
dependencies.  If you don't yet have `conda-lock` installed in your base
environment, run:

    conda install -c conda-forge -n base conda-lock=1

### Python

This needs the following Python dependencies:

- Python 3.8 or later
- numpy
- pandas
- seaborn
- jupyter
- jupytext
- dvc (2.0 or later)

The Python dependencies are defined in `environment.yml`.

### Rust

The Rust tools need Rust version 1.59 or later.  The easiest way to install this — besides Anaconda — is with
[rustup](https://www.rust-lang.org/learn/get-started).

The `cargo` build tool will automatically download all Rust libraries required.  The Rust code does not depend on any specific system libraries.

### Regenerating Lockfiles

If you update dependencies, you can re-generate the Conda lockfiles with `conda-lock`:

    conda-lock lock --mamba -f pyproject.toml
