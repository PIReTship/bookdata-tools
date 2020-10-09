This repository contains the code to import and integrate the book and rating data that we work with.
It imports and integrates data from several sources in a single PostgreSQL database; import scripts
are primarily in Python, with Rust code for high-throughput processing of raw data files.

If you use these scripts in any published research, cite [our paper](https://md.ekstrandom.net/pubs/book-author-gender):

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

We use [Data Version Control](https://dvc.org) (`dvc`) to script the import and wire
its various parts together.  A complete re-run, not including file download time, takes
approximately **8 hours** on our hardware (24-core 2GHz Xeon, 128GiB RAM, spinning disks).

## Configurating Database Access

All scripts read database configuration from the `DB_URL` environment variable, or alternately
a config file `db.cfg`.  This file should look like:

```ini
[DEFAULT]
host = localhost
database = bookdata
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
-   Amazon Ratings - the 'ratings only' data for _Books_ from <http://jmcauley.ucsd.edu/data/amazon/> and save it in `data` (**not** auto-downloaded - save CSV file in `data`).  **If you use this data, cite the paper on that site.**
-   BookCrossing - the BX-Book-Ratings CSV file from <http://www2.informatik.uni-freiburg.de/~cziegler/BX/> (auto-downloaded). **If you use this data, cite the paper on that site.**
-   GoodReads - the GoodReads books, works, authors, and *full interaction* files from <https://sites.google.com/eng.ucsd.edu/ucsdbookgraph/home> (**not** auto-downloaded - save GZip'd JSON files in `data`).  **If you use this data, cite the paper on that site.**

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

Running the scripts here with raw `dvc` **does not work**.  You need to use the `dvc.sh` wrapper
script, as in:

    ./dvc.sh repro

The wrapper script sets up DVC to recognize our special `pgstat://stage` URLs for tracking the
status of database import stages in the live database.

Import is structured as a concept of *stages* map almost 1:1 to our DVC step files.  They manage
database-side tracking of data and status.

Each import stage includes `pgstat://stage` as an *unached* output stage, as in:

``` yaml
outs:
- path: pgstat://bx-import
  cache: false
```

From the command line, uncached outptus are created by using `-O` instead of `-o`.

Each script that requires another stage to be run first depends on `pgstat://stage` as a dependency.

This wires together all of the dependencies, and uses the current state in the database instead of
files that might become out-of-sync with the database to track import status.

The stage name matches the name of the `.dvc` file.

The reason for this somewhat bizarre layoutis that if we just wrote the output files, and the database
was reloaded or corrupted, the DVC status-checking logic would not be ableto keep track of it.  This
double-file design allows us to make subsequent steps depend on the actual results of the import, not
our memory of the import in the Git repository.

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

Data integration then happens after the data sets are indexed (mostly - a few indexing steps
depend on the book clustering process).

### Adding a New Dataset

If you want to add a new data set, there are a few steps:

1.  Set up the initial raw database schema, with an SQL script and corresponding DVC file under
    `schemas/`. This should hold the data in a form that matches as closely as practical the raw
    form of the data, and should have minimal indexes and constraints.  For a new schema `ds-schema`,
    you create two files:

    -   `ds-schema.sql`, containing the CREATE SCHEMA and CREATE TABLE statements.  We use
        PostgreSQL schemas (namespaces) to separate the data from different sources to make
        the whole database more manageable.  Look at existing schema definitions for examples.
    -   `ds-schema.dvc`, the DVC file running `ds-schema.sql`.  This should contain a few things:

        ```yaml
        # Run the schema file
        cmd: python ../run.py sql-script ds-schema.sql
        # Depend on the file and initial database setup
        deps:
        - path: ds-schema.sql
        - path: pgstat://common-schema
        outs:
        # a transcript of the script run
        - path: ds-schema.transcript
        # the status of importing this schema
        - path: pgstat://ds-schema
          cache: false
        ```

        When you run `./dvc.sh repro schemas/ds-schema.dvc`, it will run the schema script and
        fill in the other values (e.g. checksums) for the dependencies and outputs.

2.  Download the raw data files into `data` and register them with DVC (`dvc add data/file` for
    the simplest case), and document in this file where to download them.  For files that it is
    reasonable to auto-download, you can create a more sophisticated setup to download them, but
    this is often not necessary.

3.  Identify, modify, and/or create the code needed to import the raw data files into the database.
    We have importers for several types of files already:

    -   If the data is in CSV or similar form, suitable for PostgreSQL's `COPY FROM` command, the
        `pcat` import tool in the Rust tools can copy the file, decompressing if necessary, directly
        to the database table.

    -   If the data is in JSON, we have importers for two forms of JSON in the `import-json` Rust
        tool, the source for which is in `src/commands/import_json/`.  Right now it supports
        OpenLibrary and GoodReads JSON files; the first is a tab-separated file containing
        object metadata and the JSON object, and the second is a simple object-per-line format.
        The accompanying file (`openlib.rs` and `goodreads.rs`) define the data format and the
        destination tables.  For many future JSON objects, `goodreads.rs` will be the appropriate
        template to start with, and add support for it to the appropriate places in `mod.rs`.

    -   If the data is in MARC-XML, the Rust `parse-marc` command is your starting place.  It can
        process both multiple-record formats (e.g. from VIAF) or single-document formats (from the
        Library of Congress), and can decompress while importing.

    If you need to write new import code, you may need to make sure it properly records stage
    dependencies and status.  At a minimum, it should record each file imported and its checksum as
    a file for the stage, along with the stage begin/end timestamps.  Look at the `meta-schema.sql`
    file for the specific tables.  The `tracking.py` and `tracking.rs` support modules provide code
    for recording stage status in Python and Rust, respectively.

4.  Set up the import process with an appropriate `.dvc` step in `import/`.  This step should depend
    on the schema (`pgstat://ds-schema`), and have as one of its uncached outputs the import process
    status (`pgstat://ds-import`, if the file is named `ds-import.dvc`).  Some importers require you
    to explicitly provide the stage name as a command-line argument.

5.  Write SQL commands to transform and index the imported data in a script under `index/`.  This script
    may do a number of things:

    -   Map data set book ISBNs or other identifiers to ISBN IDs.
    -   Extract relational tables from structured data such as JSON (e.g. the book author lists
        extracted from OpenLibrary).
    -   Create summary tables or materialized views.

    See the existing index logic in `index/` for examples.

6.  Create a `.dvc` stage to run your index script; this works like the one for the schema in
    (1), but is under `index/` and depends on `pgstat://ds-import` (or whatever your import
    stage is named).

6.  Create or update data integrations to make use of this data, as needed and appropriate.

    If the new data contains ISBN/ID links that you want to include in book clustering, add support
    to `cluster.py` and update the `cluster.dvc` file to also depend on your data set's index
    stage (e.g. `pgstat://ds-index`).

7.  If appropriate, add a dependency on the last stage of your processing to `Dvcfile`.

All dependencies should be through the `pgstat://` URLs, so that they are computed from current
database status.

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

## Copyright and Acknowledgements

Copyright &copy; 2020 Boise State University.  Distributed under the MIT License; see LICENSE.md.
This material is based upon work supported by the National Science Foundation under
Grant No. IIS 17-51278. Any opinions, findings, and conclusions or recommendations
expressed in this material are those of the author(s) and do not necessarily reflect
the views of the National Science Foundation.
