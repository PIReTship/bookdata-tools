---
title: VIAF
parent: Data Model
nav_order: 4
---

# Virtual Internet Authority File

We source author data from the [Virtual Internet Authority File](http://viaf.org), as downloaded from
their [data dumps](http://viaf.org/viaf/data).  This file is slow and error-prone to download, and is
not* auto-downloaded.

Imported data lives under the `viaf` directory.

## Import Steps

The import is controlled by the following DVC steps:

`scan-authors`
:   Import the VIAF MARC data into {file}`viaf/viaf.parquet`.

`author-names`
:   Extract author names from the VIAF MARC data, producing `author-names.csv.gz`.

`author-genders`
:   Extract author genders from the VIAF MARC data, producing {file}`author-genders.parquet`.

`index-names`
:   Normalize and expand author names and map to VIAF record IDs, producing {file}`author-index-names.parquet`.

## Raw Data

The VIAF data is in [MARC 21 Authority Record format](https://www.loc.gov/marc/authority/).  The initial
scan stage extracts this into a table using the [MARC schema](marc-format).

:::{file} viaf/viaf.parquet

The table storing raw MARC fields from VIAF.
:::

## Extracted Author Tables

We process the MARC records to produce several derived tables.

:::{file} viaf/author-name-index.parquet

The author-name index file maps record IDs to author names, as defined in field [700a][].  For each record, it stores each of the
names extracted by {rust:mod}`bookdata::cleaning::names`.
:::

:::{file} viaf/author-genders.parquet

This file contains the extracted gender information for each author record (field [375a][]).  If a record has multiple
gender fields, they are all recorded.  Merging gender records happens later in the integration.
:::

[700a]: https://www.loc.gov/marc/authority/ad700.html
[375a]: https://www.loc.gov/marc/authority/ad375.html

## VIAF Gender Vocabulary

The MARC [gender field][375a] is defined as the author's gender *identity*.  It
allows identities from an open vocabulary, along with start and end dates for
the validity of each identity.

The Program for Cooperative Cataloging Task Group on Gender in Name Authority Records produced a
[report](https://www.loc.gov/aba/pcc/documents/Gender_375%20field_RecommendationReport.pdf) with
recommendations for how to record this field.  Many libraries contributing to the Library of Congress
file, from which many VIAF records are sourced, follow these recommendations, but it is not safe
to assume they are universally followed by all VIAF contributors.

Further, as near as we can tell, the VIAF removes all non-binary gender identities or converts them
to ‘unknown’.

This data should only be used with great care.  We discuss these limitations in
[the extended paper](https://md.ekstrandom.net/pubs/bag-extended).
