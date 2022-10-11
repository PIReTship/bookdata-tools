---
title: GoodReads
parent: Data Model
---

# GoodReads (UCSD Book Graph)

We import GoodReads data from the [UCSD Book Graph](https://sites.google.com/eng.ucsd.edu/ucsdbookgraph/home)
for additional book and user interaction information.  The source files are not automatically downloaded; you
will need the following:

- Books
- Book works
- Authors
- Book genres
- Book series
- **Full** interaction data

We do not yet support reviews, or the CSV summary of the interaction data.

**If you use this data, cite the paper(s) documented on the data set web site.**

:::{index} pair: directory; gr
:::

Imported data lives in the `gr` directory.

## Import Steps

The import is controlled by several DVC steps:

`scan-*`
:   The various `scan-*` steps each scan a JSON file into corresponding Parquet files.  They have a specific order, as scanning interactions needs book information.

`book-isbn-ids`
:   Match GoodReads ISBNs with ISBN IDs.

`book-links`
:   Creates {file}`goodreads/gr-book-link.parquet`, which links each GoodReads book with its work (if applicable) and is cluster ID.

`cluster-actions`
:   Extracts cluster-level implicit feedback data.  Each (user, cluster) pair has one record, with the number of actions (the number of times the user added a book from that cluster to a shelf) and timestamp data.

`cluster-ratings`
:   Extracts cluster-level explicit feedback data.  This is the ratings each user assigned to books in each cluster.

`work-actions`, `work-ratings`
:   The same thing as the `cluster-*` stages, except it groups by GoodReads work instead of by integrated cluster. If you are only working with the GoodReads data, and not trying to connect across data sets, this data is better to work with.

`work-gender`
:   The author gender for each GoodReads work, as near as we can tell.

## Scanned and Linking Data

:::{file} goodreads/gr-book-ids.parquet

Identifiers extracted from each GoodReads book record.
:::

:::{file} goodreads/gr-book-info.parquet

Metadata extracted from GoodReads book records.
:::

:::{file} goodreads/gr-book-genres.parquet

GoodReads book-genre associations.
:::

:::{file} goodreads/gr-book-series.parquet

GoodReads book series associations.
:::

:::{file} goodreads/gr-genres.parquet

The genre labels to go with {file}`goodreads/gr-book-genres.parquet`.
:::

:::{file} goodreads/gr-book-link.parquet

Linking identifiers (work and cluster) for GoodReads books.
:::

:::{file} goodreads/gr-work-info.parquet

Metadata extracted from GoodReads work records.
:::

:::{file} goodreads/gr-interactions.parquet

GoodReads interaction records.
:::

## Cluster-Level Tables

:::{file} goodreads/gr-cluster-actions.parquet

Cluster-level implicit-feedback records, suitable for use in LensKit. The `item` column contains cluster IDs.
:::

:::{file} goodreads/gr-cluster-ratings.parquet

Cluster-level explicit-feedback records, suitable for use in LensKit. The `item` column contains cluster IDs.
:::

## Work-Level Tables

:::{file} goodreads/gr-work-actions.parquet

Work-level implicit-feedback records, suitable for use in LensKit. The `item` column contains work IDs.
:::

:::{file} goodreads/gr-work-ratings.parquet

Work-level explicit-feedback records, suitable for use in LensKit. The `item` column contains work IDs.
:::

:::{file} goodreads/gr-work-gender.parquet

Author gender for GoodReads works.  This is computed by connecting works to clusters and obtaining the cluster gender information from {file}`book-links/cluster-genders.parquet`.
:::
