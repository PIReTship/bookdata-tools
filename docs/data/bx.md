---
title: BookCrossing
parent: Data Model
nav_order: 5
---

# BookCrossing

The [BookCrossing data set](http://www2.informatik.uni-freiburg.de/~cziegler/BX/) consists of user-provided
ratings — both implicit and explicit — of books.

**If you use this data, cite:**

> Cai-Nicolas Ziegler, Sean M. McNee, Joseph A. Konstan, and Georg Lausen. 2005. Improving Recommendation Lists Through Topic Diversification. <cite>Proceedings of the 14th International World Wide Web Conference</cite> (WWW '05), May 10-14, 2005, Chiba, Japan. DOI:[10.1145/1060745.1060754](https://doi.org/10.1145/1060745.1060754).

:::{index} pair: directory; bx
:::

Imported data lives in the `bx` directory.  The source data files are automatically downloaded and unpacked by
the provided scripts and DVC stages.

## Import Steps

The import is controlled by the following DVC steps:

`data/BX-CSV-Dump.zip.dvc`
:   Download the BookCrossing zip file.

`clean-ratings`
:   Unpack ratings from the downloaded zip file and clean up their invalid characters.

`cluster-ratings`
:   Combine BookCrossing ratings with [book clusters](cluster) to produce (user, cluster, rating) from the explicit-feedback ratings. BookCrossing implicit feedback entries (rating of 0) are excluded. Produces {file}`bx/bx-cluster-ratings.parquet`.

`cluster-actions`
:   Combine BookCrossing interactions with [book clusters](cluster) to produce (user, cluster) implicit-feedback records. These records include the BookCrossing implicit feedback entries (rating of 0). Produces {file}`bx/bx-cluster-actions.parquet`.

## Raw Data

The raw rating data, with invalid characters cleaned up, is in the {file}`bx/cleaned-ratings.csv` file.

:::{file} bx/cleaned-ratings.csv

Cleaned-up, but not integrated, book ratings.  Has the following columns:

user_id
:   The user identifier (numeric).

isbn
:   The book ISBN (text).

rating
:   The book rating {math}`r_{ui}`.  The ratings are on a 1-10 scale, with 0 indicating an implicit-feedback record.
:::

## Extracted Actions

:::{file} bx/bx-cluster-ratings.parquet

The explicit-feedback ratings ({math}`r_{ui}>0` from {file}`bx/cleaned-ratings.csv`), with book clusters as the `item`s.
:::

:::{file} bx/bx-cluster-actions.parquet

All user-item interactions from {file}`bx/cleaned-ratings.csv`, with book clusters as the `item`s.
:::
