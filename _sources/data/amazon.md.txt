---
title: Amazon
parent: Data Model
nav_order: 6
---

# Amazon Ratings

The [Amazon reviews data set](http://jmcauley.ucsd.edu/data/amazon/) consists of user-provided
reviews and ratings for a variety of products.

Currently we import the ratings-only data from the Books segment of the 2014 data set.  Future versions of the data tools will support reviews and the more 2018 data set.

**If you use this data, cite the paper(s) documented on the data set web site:**

> R. He and J. McAuley. 2016. Ups and downs: Modeling the visual evolution of fashion trends with one-class collaborative filtering. In <cite>Proc. WWW 2016</cite>. DOI:[10.1145/2872427.2883037](https://dx.doi.org/10.1145/2872427.2883037).

> J. McAuley, C. Targett, J. Shi, and A. van den Hengel. Image-based recommendations on styles and substitutes. In <cite>Proc. SIGIR 2016</cite>. DOI:[10.1145/2766462.2767755](http://dx.doi.org/10.1145/2766462.2767755).

:::{index} pair: directory; az2014
:::

Imported data lives in the `az2014` directory.  The source files are not automatically downloaded.

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

## Extracted Rating Tables

:::{file} az2014/az-cluster-ratings.parquet

This file contains the integrated Amazon ratings, with cluster IDs in the `item` column.
:::
