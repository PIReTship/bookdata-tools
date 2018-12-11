This repository contains the code to import and integrate the book and rating data that we work with.

## Requirements

- PostgreSQL 10 or later with [orafce](https://github.com/orafce/orafce)
- Python 3.6 or later with the following packages:
    - psycopg2
    - invoke
    - numpy
    - tqdm
    - pandas
    - sqlalchemy
    - numba
- A Rust compiler
- `psql` executable on the machine where the import scripts will run
- 300GB disk space for the database
- 20-30GB disk for data files

The `environment-linux-x64.yml` file defines an Anaconda environment that contains all the required
packages, with the exception of the PostgreSQL server and client executables.

All scripts read database connection info from the standard PostgreSQL client environment variables:

- `PGDATGABASE`
- `PGHOST`
- `PGUSER`
- `PGPASSWORD`

## Running Import Tasks

The import process is scripted with [invoke](http://www.pyinvoke.org).  The first tasks to run are
the import tasks:

    invoke loc.import
    invoke viaf.import
    invoke openlib.import-authors openlib.import-works openlib.import-editions
    invoke goodreads.import
    invoke ratings.import-az
    invoke ratings.import-bx

Once all the data is imported, you can begin to run the indexing and linking tasks:

    invoke viaf.index
    invoke loc.index
    invoke openlib.index
    invoke goodreads.index-books
    invoke analyze.cluster-loc
    invoke analyze.cluster-ol
    invoke analyze.cluster-gr
    invoke analyze.cluster
    invoke ratings.index
    invoke goodreads.index-ratings
    invoke analyze.authors

The tasks keep track of the import status in an `import_status` table, and will
keep you from running tasks in the wrong order.

## Setting Up Schemas

The `-schema` files contain the base schemas for the data:

- `common-schema.sql` — common tables
- `loc-schema.sql` — Library of Congress catalog tables
- `ol-schema.sql` — OpenLibrary book data
- `viaf-schema.sql` — VIAF tables
- `az-schema.sql` — Amazon rating schema
- `bx-schema.sql` — BookCrossing rating data schema
- `gr-schema.sql` — GoodReads data schema
