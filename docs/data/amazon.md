---
title: Amazon
parent: Data Model
nav_order: 6
---

# Amazon Ratings

This processes two data sets from Julian McAuley's group at UCSD:

- The [2014 Amazon reviews data set](http://jmcauley.ucsd.edu/data/amazon/)
- The [2018 Amazon reviews data set](https://nijianmo.github.io/amazon/index.html)

Each consists of user-provided reviews and ratings for a variety of products.

Currently we import the ratings-only data from the Books segment of the 2014 and 2018 data sets.  Future versions of the data tools will support reviews.

**If you use this data, cite the paper(s) documented on the data set web site.**  For 2014 data:

> R. He and J. McAuley. 2016. Ups and downs: Modeling the visual evolution of fashion trends with one-class collaborative filtering. In <cite>Proc. WWW 2016</cite>. DOI:[10.1145/2872427.2883037](https://dx.doi.org/10.1145/2872427.2883037).

> J. McAuley, C. Targett, J. Shi, and A. van den Hengel. Image-based recommendations on styles and substitutes. In <cite>Proc. SIGIR 2016</cite>. DOI:[10.1145/2766462.2767755](http://dx.doi.org/10.1145/2766462.2767755).

For 2018 data:

> J. Ni, J. Li, and J. McAuley. Justifying recommendations using distantly-labeled reviews and fined-grained aspects. In <cite>Empirical Methods in Natural Language Processing (EMNLP), 2019</cite>.

:::{index} pair: directory; az2014
:::
:::{index} pair: directory; az2018
:::

Imported data lives in the `az2014` and `az2018` directories.  The source files
are not automatically downloaded — you will need to download the
**ratings-only** data for the Books category from each data site and save them
in the `data/az2014` and `data/az2018` directories.

## Import Steps

The import is controlled by the following DVC steps:

`scan-ratings`
:   Scan the rating CSV file into a Parquet file, converting user strings into numeric IDs.  Produces {file}`az2014/ratings.parquet`.

`cluster-ratings`
:   Link ratings with book clusters and aggregate by cluster, to produce user ratings for book clsuters.  Produces {file}`az2014/az-cluster-ratings.parquet`.

## Raw Data

:::{file} az2014/ratings.parquet

The raw rating data, with user strings converted to numeric IDs, is in this file.
:::

:::{file} az2018/ratings.parquet

The raw rating data, with user strings converted to numeric IDs, is in this file.
:::

## Extracted Rating Tables

:::{file} az2014/az-cluster-ratings.parquet

This file contains the integrated Amazon ratings, with cluster IDs in the `item` column.
:::

:::{file} az2018/az-cluster-ratings.parquet

This file contains the integrated Amazon ratings, with cluster IDs in the `item` column.
:::
