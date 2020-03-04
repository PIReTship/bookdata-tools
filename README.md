This repository contains the code to import and integrate the book and rating data that we work with.
It imports and integrates data from several sources in a single PostgreSQL database; import scripts
are primarily in Python, with Rust code for high-throughput processing of raw data files.

If you use these scripts in any published reseaerch, cite [our paper](https://md.ekstrandom.net/pubs/book-author-gender):

> Michael D. Ekstrand, Mucun Tian, Mohammed R. Imran Kazi, Hoda Mehrpouyan, and Daniel Kluver. 2018. Exploring Author Gender in Book Rating and Recommendation. In *Proceedings of the 12th ACM Conference on Recommender Systems* (RecSys '18). ACM, pp. 242–250. DOI:[10.1145/3240323.3240373](https://doi.org/10.1145/3240323.3240373). arXiv:[1808.07586v1](https://arxiv.org/abs/1808.07586v1) [cs.IR].

**Note:** the limitations section of the paper contains important information about
the limitations of the data these scripts compile.  **Do not use the gender information
in this data data or tools without understanding those limitations**.  In particular,
VIAF's gender information is incomplete and, in a number of cases, incorrect.

In addition, several of the data sets integrated by this project come from other sources
with their own publications.  **If you use any of the rating or interaction data, cite the
appropriate original source paper.**  For each data set below, we have provided a link to the
page that describes the data and its appropriate citation.

## Requirements

- PostgreSQL 10 or later with [orafce](https://github.com/orafce/orafce) and `pg_prewarm` (from the
  PostgreSQL Contrib package) installed.
- Python 3.6 or later with the following packages:
    - psycopg2
    - numpy
    - tqdm
    - pandas
    - colorama
    - chromalog
    - natural
    - dvc
- The Rust compiler (available from Anaconda)
- 2TB disk space for the database
- 100GB disk space for data files

It is best if you do not store the data files on the same disk as your PostgreSQL database.

The `environment.yml` file defines an Anaconda environment that contains all the required packages except for the PostgreSQL server. It can be set up with:

    conda create -f environment.yml

All scripts read database connection info from the standard PostgreSQL client environment variables:

- `PGDATABASE`
- `PGHOST`
- `PGUSER`
- `PGPASSWORD`

Alternatively, they will read from `DB_URL`.

We use [Data Version Control](https://dvc.org) (`dvc`) to script the import and wire
its various parts together.  A complete re-run, not including file download time, takes
approximately **8 hours** on our hardware (24-core 2GHz Xeon, 128GiB RAM, spinning disks).

## Initializing and Configuring the Database

After creating your database, initialize the extensions (as the database superuser):

    CREATE EXTENSION orafce;
    CREATE EXTENSION pg_prewarm;
    CREATE EXTENSION "uuid-ossp";

The default PostgreSQL performance configuration settings will probably not be
very effective; we recommend turning on parallelism and increasing work memory,
at a minimum.

## Downloading Data Files

This imports the following data sets:

-   Library of Congress MDSConnect Open MARC Records from <https://www.loc.gov/cds/products/MDSConnect-books_all.html> (auto-downloaded).
-   LoC MDSConnect Name Authorities from <https://www.loc.gov/cds/products/MDSConnect-name_authorities.html> (auto-downloaded).
-   Virtual Internet Authority File - get the MARC 21 XML data file from <http://viaf.org/viaf/data/> (**not** auto-downloaded).
-   OpenLibrary Dump - the editions, works, and authors dumps from <https://openlibrary.org/developers/dumps> (auto-downloaded).
-   Amazon Ratings - the 'ratings only' data for _Books_ from <http://jmcauley.ucsd.edu/data/amazon/> and save it in `data` (auto-downloaded).  **If you use this data, cite the paper on that site.**
-   BookCrossing - the BX-Book-Ratings CSV file from <http://www2.informatik.uni-freiburg.de/~cziegler/BX/> (auto-downloaded). **If you use this data, cite the paper on that site.**
-   GoodReads - the GoodReads books, works, authors, and *full interaction* files from <https://sites.google.com/eng.ucsd.edu/ucsdbookgraph/home> (**not** auto-downloaded).  **If you use this data, cite the paper on that site.**

Several of these files can be auto-downloaded with the DVC scripts; others will need to be manually downloaded.

## Running Everything

You can run the entire import process with:

    dvc repro

Individual steps can be run with their corresponding `.dvc` files.

## Layout

The import code consists of Python, Rust, and SQL code, wired together with DVC.

### Python Scripts

Python scripts live under `scripts`, as a Python package.  They should not be launched directly, but
rather via `run.py`, which will make sure the environment is set up properly for them:

    python run.py sql-script [options] script.sql

### SQL Scripts

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

### DVC Usage and Stage Files

In order to allow DVC to be aware of current database state, we use a little bit of an unconventional
layout for many of our DVC scripts.  Many steps have two `.dvc` files with associated outputs:

-   `step.dvc` runs import stage `step`.
-   `step.transcript` is (consistent) output from running `step`, recording the actions taken.  It is
    registered with DVC as the output of `step.dvc`.
-   `step.status.dvc` is an *always-changed* DVC stage that depends on `step.transcript` and produces
    `step.status`, to check the current status in the database of that import stage.
-   `step.status` is an *uncached* output (so it isn't saved with DVC, and we also ignore it from Git)
    that is registered as the output of `step.status.dvc`.  It contains a stable status dump from the
    database, to check whether `step` is actually in the database or has changed in a meaningful way.

Steps that depend on `step` then depend on `step.status`, *not* `step.transcript`.

The reason for this somewhat bizarre layoutis that if we just wrote the output files, and the database
was reloaded or corrupted, the DVC status-checking logic would not be ableto keep track of it.  This
double-file design allows us to make subsequent steps depend on the actual results of the import, not
our memory of the import in the Git repository.

The file `init.status` is an initial check for database initialization, and forces the creation of the
meta-structures used for tracking stage status.  Everything touching the database should depend on it,
directly or indirectly.

### In-Database Status Tracking

Import steps are tracked in the `stage_status` table in the database.  For completed stages, this can
include a key (checksum, UUID, or other identifier) to identify a 'version' of the stage.  Stages
can also have dependencies, which are solely used for computing the status of a stage (all actual
dependency relationships are handled by DVC):

- `stage_deps` tracks stage-to-stage dependencies, to say that one stage used another as input.
- `stage_file` tracks stage-to-file dependencies, to say that a stage used a file as input.

The `source_file` table tracks input file checksums.

Projects using the book database can also use `stage_status` to obtain data version information, to
see if they are up-to-date.

### Utility Code

The `bookdata` package contains Python utility code, and the `src` directory contains a number
of utility modules for use in the Rust code.  To the extent reasonable, we have tried to mirror
design patterns and function names.

## Design for Datasets

The general import philosophy is that we import the data into a PostgreSQL table in a raw form,
only doing those conversions necessary to be able to access it with PostgreSQL commands and
functions.  We then extract information from this raw form into other tables and materialized
views to enable relational queries.

Each data set's import process follows the following steps:

1. Initialize the database schema, with an SQL script under `schemas/`.
2. Import the raw data, controlled by DVC steps under `import/`.  This may be multiple steps;
   for example, OpenLibrary has a separate import step for each file.  Actual import is usually
   handled by Rust or Python code.
3. Index the data into relational views and tables.  This is done by SQL scripts under `index/`.

Data integration then happens after the data sets are indexed (mostly).

## Book Identifiers

Each data set comes with its own identification scheme for books:

- LCCN
- OpenLibrary key
- ASIN
- GoodReads book and work identifiers

We integrate these through two steps.  First, we map ISBNs to numeric IDs with the `isbn_id` table.
This table contains every ISBN (or ISBN-like thing, such as ASIN) we have seen and associates it
with a unique identifier.

Second, we map ISBN IDs to clusters with the `isbn_cluster` table.  A cluster is a collection of
related ISBNs, such as the different editions of a work.  They correspond to GoodReads or OpenLibrary
'works' (in fact, when a GoodReads or OpenLibrary work is available, it is used to generate the
clusters).

This allows us to connect ratings to metadata with maximal link coverage, by pulling in metadata
across the whole book cluster.
