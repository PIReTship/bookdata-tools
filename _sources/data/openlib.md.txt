---
title: OpenLibrary
parent: Data Model
---

# OpenLibrary

We also source book data from [OpenLibrary](https://openlibrary.org), as downloaded from
their [developer dumps](https://openlibrary.org/developers/dumps).

The DVC control files automatically download the appropriate version.  The version can be
updated by modifying the `data/ol_dump_*.txt.gz.dvc` files.

:::{index} pair: directory; openlibrary
:::

Imported data lives under the `openlibrary` directory.

```{mermaid}
erDiagram
    editions ||--o{ edition-isbn-ids : ""
    edition-isbn-ids }o--|| all-isbns : ""
    editions {
        Int32 id PK
        Utf8 key
        Utf8 title
    }
    editions }o--o{ works : "edition-works"
    editions |o--o{ edition-subjects : ""
    edition-subjects {
        Int32 id
        Utf8 subject
    }
    works {
        Int32 id PK
        Utf8 key
        Utf8 title
    }
    works |o--o{ work-subjects : ""
    work-subjects {
        Int32 id
        Utf8 subject
    }
    authors {
        Int32 id PK
        Utf8 key
        Utf8 name
    }
    authors ||--o{ author-names : ""
    editions }o--o{ authors : "edition-authors"
    works }o--o{ authors : "work-authors"
```

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

The raw data lives in the `data/openlib` directory, as compressed JSON files.  Right now we do not
extract very many fields from OpenLibrary; additional fields can be extracted by extending the
import scripts.

## Extracted Edition Tables

We extract the following tables from OpenLibrary editions:

:::{file} openlibrary/editions.parquet

This file contains a primary record for each edition, with the numeric edition ID, OpenLibrary key,
and edition data.
:::

:::{file} openlibrary/edition-authors.parquet

This file contains mappings between editions and their authors.
:::

:::{file} openlibrary/edition-works.parquet

This maps editions to their works.
:::

:::{file} openlibrary/edition-isbns.parquet

This contains the ISBN fields extracted from each OpenLibrary edition.  This is
primarily for internal purposes and most people won't need to use it.  ISBNs are
cleaned (with {rust:fn}`~bookdata::cleaning::isbns::clean_isbn_chars` or
{rust:fn}`~bookdata::cleaning::isbns::clean_asin_chars`) prior to being stored in this
file.
:::

:::{file} openlibrary/edition-subjects.parquet

This table contains the subjects for OpenLibrary editions.  Each row contains an edition ID and one subject.
Its schema is in {rust:struct}`~bookdata::openlib::edition::EditionSubjectRec`.
:::

:::{file} openlibrary/edition-isbn-ids.parquet

This file maps editions to numeric [ISBN identifiers](isbn-id).  It is derived
from {file}`openlibrary/edition-isbns.parquet`.
:::

## Extracted Work Tables

We extract the following tables from OpenLibrary works:

:::{file} openlibrary/works.parquet

This file contains the primary record for each work, mapping a numeric ID to its OpenLibrary key and containing
other per-work fields.
:::

:::{file} openlibrary/work-authors.parquet

This file links work records to the work's author list (works may have separate author lists from their editions).
:::

:::{file} openlibrary/work-subjects.parquet

This table contains the subjects for OpenLibrary editions.  Each row contains an edition ID and one subject.
Its schema is in {rust:struct}`~bookdata::openlib::work::WorkSubjectRec`.
:::

## Extracted Author Tables

:::{file} openlibrary/authors.parquet

This file contains basic information about OpenLibrary authors.
:::

:::{file} openlibrary/author-names.parquet

This file contains the names associated with each author in {file}`openlibrary/authors.parquet`.
:::

## Utility Tables

:::{file} openlibrary/work-clusters.parquet

This file is a helper table to make it easier to connect OpenLibrary data to clusters by mapping
OpenLibrary work IDs to book data cluster IDs.
:::
