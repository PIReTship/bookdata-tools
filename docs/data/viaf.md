---
title: VIAF
parent: Data Model
nav_order: 4
---

# Virtual Internet Authority File
{: .no_toc}

We source author data from the [Virtual Internet Authority File](http://viaf.org), as downloaded from
their [data dumps](http://viaf.org/viaf/data).  This file is slow and error-prone to download, and is
not* auto-downloaded.

Imported data lives in the `viaf` schema.

1. TOC
{:toc}

## Import Steps

The import is controlled by the following DVC steps:

`schemas/viaf-schema.dvc`
:   Run `viaf-schema.sql` to set up the base schema.

`import/viaf.dvc`
:   Import raw VIAF MARC data from `data/viaf-clusters-marc21.xml.gz`.

`index/viaf-index.dvc`
:   Run `viaf-index.sql` to index the MARC data and extract tables.

## Raw Data

VIAF data is in [MARC 21 Authority Record format](https://www.loc.gov/marc/authority/).  The raw
MARC data is imported into the `marc_field` table with the [same format as LOC](loc.html#raw).

## Extracted Author Tables

We extract the following tables for VIAF authors:

`author_name`
:   The author's name(s).  We insert an author name for each field with tag 700 and subfield code ‘a’.
    For all author names of the form ‘Family, Given’, we insert an additional record with the form
    ‘Given Family’ and indicator ‘S’.  This helps maximize links.

`author_gender`
:   The author's gender, from field 375 subfield ‘a’.  This is a raw extract of all gender identity
    assertions in the record; we resolve multiple assertions later in the data integration process.

## VIAF Gender Vocabulary

The MARC [gender field](https://www.loc.gov/marc/authority/ad375.html) is defined as the author's
gender *identity*.  It allows identities from an open vocabulary, along with start and end dates
for the validity of each identity.

The Program for Cooperative Cataloging Task Group on Gender in Name Authority Records produced a
[report](https://www.loc.gov/aba/pcc/documents/Gender_375%20field_RecommendationReport.pdf) with
recommendations for how to record this field.  Many libraries contributing to the Library of Congress
file, from which many VIAF records are sourced, follow these recommendations, but it is not safe
to assume they are universally followed by all VIAF contributors.

Further, as near as we can tell, the VIAF removes all non-binary gender identities or converts them
to ‘unknown’.

This data should only be used with great care.  We discuss these limitations in [the extended
preprint](https://md.ekstrandom.net/pubs/bag-extended).
