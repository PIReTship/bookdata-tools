---
title: Dataset Design
parent: Implementation
nav_order: 2
---

# Design for Datasets

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

## Adding a New Dataset

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
