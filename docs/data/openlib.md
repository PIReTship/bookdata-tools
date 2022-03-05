---
title: OpenLibrary
parent: Data Model
nav_order: 3
---

# OpenLibrary

We also source book data from [OpenLibrary](https://openlibrary.org), as downloaded from
their [developer dumps](https://openlibrary.org/developers/dumps).

The DVC control files automatically download the appropriate version.  The version can be
updated by modifying the `data/ol_dump_*.txt.gz.dvc` files.

:::{index} pair: directory; openlibrary
:::

Imported data lives under the `openlibrary` directory.

## Import Steps

The import is controlled by the following DVC steps:

`scan-*`
:   The various `scan-*` stages (e.g. `scan-authors`) scan an OpenLibrary JSON file into the
    resulting Parquet files.  There are dependencies, to resolve OpenLibrary keys to numeric
    identifiers for cross-referencing.  These scan stages do not currently extract all available
    data from the OpenLibrary JSON; they only extract the fields we currently use, and need to
    be extended to extract and save additional fields.

`edition-isbn-ids`
:   Convert edition ISBNs into [ISBN IDs](isbn-id), producing {file}`openlib/edition-isbn-ids.parquet`.

## Raw Data

OpenLibrary provides its data as JSON.  It is imported as-is into a JSONB column in three tables:

- `ol.author`
- `ol.work`
- `ol.edition`

Each of these has the following columns:

*type*_id
:    A numeric record identifier generated at import.

*type*_key
:    The OpenLibrary identifier key (e.g. `/books/3180A3`).

*type*_data
:    The raw JSON data containing the record.

We use PostgreSQL's JSON operators and functions to extract the data from these tables for the
rest of the OpenLibrary data model.

## Extracted Edition Tables

We extract the following tables from OpenLibrary editions:

`edition_author`
:   Links `edition` and `author` to record an edition's authors.

`edition_first_author`
:   Links `edition` and `author` to record an edition's first author.

`edition_work`
:   Links each `edition` to its `work`(s)

`edition_isbn`
:   The raw ISBNs for each `edition` (*not* ISBN IDs)

`isbn_link`
:   Link ISBNs, editions, and works, along with the book code derived from an edition's
    work and edition IDs.  If an edition belongs to multiple works, it will appear multiple
    times here.  This table violates 4NF.

## Extracted Work Tables

We extract the following tables from OpenLibrary works:

`work_author`
:   Links `work` and `author` to record an work's authors.

`work_first_author`
:   Links `work` and `author` to record an work's first author.

`work_subject`
:   The `subjects` entries for each work.

## Extracted Author Tables

:::{file} openlibrary/authors.parquet

This file contains basic information about OpenLibrary authors.
:::

:::{file} openlibrary/author-names.parquet

This file contains the names associated with each author in {file}`openlibrary/authors.parquet`.
:::
