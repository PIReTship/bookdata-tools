---
title: Setup
parent: Importing
nav_order: 2
---

# Setting Up the Environment
{: .no_toc}

These tools require PostgreSQL and an Anaconda installation.

1. TOC
{:toc}

## System Requirements

You will need:

- A PostgreSQL server with at least 500GB of disk space available for the database and ample RAM.
- An environment to save the raw files and run the import code.  This can be the same machine as the PostgreSQL server, but needs:
  - Anaconda or Miniconda
  - 100GB of disk space for input files
  - A few GB of memory
  - Linux or macOS (since graph-tool isn't built for Windows right now)

The scripts don't have substantial memory requirements, but do need a good deal of disk space.
The most memory-intensive operation is the connected components computation for book clustering.

## PostgreSQL Database

The book data tools require PostgreSQL (at least version 10), with the following extensions installed:

* [orafce](https://github.com/orafce/orafce)
* PostgreSQL Contrib (specifically `pg_prewarm` and `uuid-ossp`)

The database will take approximately 500GB.

Create a database for the book data, e.g. `bookdata`, owned by the database user you will be using to run the data integration tools.  The tools will create various tables and schemas.

Once you have created the database, run the following as the database superuser to enable the PostgreSQL extensions:

```sql
CREATE EXTENSION pg_prewarm;
CREATE EXTENSION orafce;
CREATE EXTENSION "uuid-ossp";
```

## Import Tool Dependencies

The import tools are written in Python and Rust.  The provided `environment.yml` file defines an Anaconda environment (named `bookdata` by default) that contains all required runtimes and libraries:

    conda env create -f environment.yml
    conda activate bookdata

If you don't want to use Anaconda, see the following for more details on dependencies.

### Python

This needs the following Python dependencies:

- Python 3.6 or later
- psycopg2
- numpy
- tqdm
- pandas
- colorama
- chromalog
- natural
- dvc (0.90 or later)
- sqlparse
- sqlalchemy

### Rust

The Rust tools need Rust version 1.40 or later.  The easiest way to install this — besides Anaconda — is with
[rustup](https://www.rust-lang.org/learn/get-started).

The `cargo` build tool will automatically download all Rust libraries required.  The Rust code does not depend on any specific system libraries.

## Database Configuration

All scripts read database configuration from the `DB_URL` environment variable, or alternately
a config file `db.cfg`.  This file should look like:

```ini
[DEFAULT]
host = localhost
database = bookdata
user = user
password = password
```

This file additionally supports branch-specfic configuration sections that will apply to work
on different Git branches, e.g.:

```ini
[DEFAULT]
host = localhost
database = bookdata

[master]
database = bdorig
```

This setup will use `bookdata` for most branches, but will connect to `bdorig` when working
from the `master` branch in the git repository.

This file should **not** be committed to Git.  It is ignored in `.gitignore`.
