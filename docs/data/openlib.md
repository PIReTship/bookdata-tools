---
title: OpenLibrary
parent: Data Model
nav_order: 3
---

# OpenLibrary
{: .no_toc}

We also source book data from [OpenLibrary](https://openlibrary.org), as downloaded from
their [developer dumps](https://openlibrary.org/developers/dumps).

The DVC control files automatically download the appropriate version.  The version can be
updated by modifying the `data/ol_dump_*.txt.gz.dvc` files.

Imported data lives in the `ol` schema.

1. TOC
{:toc}

## Import Steps

The import is controlled by the following DVC steps:

`schemas/ol-schema.dvc`
:   Run `ol-schema.sql` to set up the base schema.

`import/ol-works.dvc`
:   Import raw OpenLibrary works from `data/ol_dump_works.txt.gz`.

`import/ol-editions.dvc`
:   Import raw OpenLibrary editions from `data/ol_dump_editions.txt.gz`.

`import/ol-authors.dvc`
:   Import raw OpenLibrary authors from `data/ol_dump_authors.txt.gz`.

`index/ol-index.dvc`
:   Run `ol-index.sql` to index the book data and extract tables.

`index/ol-book-info.dvc`
:   Run `ol-book-info.sql` to extract additional book data into tables.

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

`author_name`
:   The names for each author.  An author may have more than one listed name; this extracts
    all of them.
