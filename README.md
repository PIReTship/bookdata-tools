This repository contains the code to import and integrate the book and rating data that we work with.

## Requirements

- PostgreSQL 10 (9.x may also work)
- Node.js (tested on Carbon, the 8.x LTS line)
- R with the Tidyverse and RPostgreSQL
- `psql` executable on the machine where the import scripts will run
- 300GB disk space for the database
- 20-30GB disk for data files

All scripts read database connection info from the standard PostgreSQL client environment variables:

- `PGDATGABASE`
- `PGHOST`
- `PGUSER`
- `PGPASSWORD`

## Setting Up Schemas

The `-schema` files contain the base schemas for the data to import:

- `common-schema.sql` — common tables
- `loc-schema.sql` — Library of Congress catalog tables
- `ol-schema.sql` — OpenLibrary book data
- `viaf-schema.sql` — VIAF tables
- `az-schema.sql` — Amazon rating schema
- `bx-schema.sql` — BookCrossing rating data schema

## Importing Data

The importer is run with Gulp.

    npm install
    npx gulp importOpenLib
    npx gulp importLOC
    npx gulp importVIAF
    npx gulp importBX
    npx gulp importAmazon

The full import takes 1–3 days.

## Indexing and Integrating

Start tying the data together:

    psql <viaf-index.sql
    psql <loc-index.sql
    psql <ol-index.sql

Clustering is done by the `ClusterISBNs.r` script:

    Rscript ClusterISBNs.r

With the clusters in place, we're ready to index the rating data:

    psql <az-index.sql
    psql <bx-index.sql

And finally, compute author information for ISBN clusters:

    psql <author-info.sql
