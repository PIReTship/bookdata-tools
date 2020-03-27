---
title: Source Data
parent: Usage
nav_order: 3
---

# Downloading Source Data

These import tools will integrate several data sets. Some of them are auto-downloaded, but others you will
need to download yourself and save in the `data` directory.  The data sources are:

-   [Library of Congress MDSConnect Open MARC Records](https://www.loc.gov/cds/products/MDSConnect-books_all.html) (auto-downloaded).
-   [LoC MDSConnect Name Authorities](https://www.loc.gov/cds/products/MDSConnect-name_authorities.html) (auto-downloaded).
-   [Virtual Internet Authority File](http://viaf.org/viaf/data/) MARC 21 XML data (**not** auto-downloaded).
-   [OpenLibrary Dump](https://openlibrary.org/developers/dumps) (auto-downloaded).
-   [Amazon Ratings](http://jmcauley.ucsd.edu/data/amazon/) 'ratings only' data for _Books_ (**not** auto-downloaded — save CSV file in `data`).  **If you use this data, cite the paper on that site.**
-   [BookCrossing](http://www2.informatik.uni-freiburg.de/~cziegler/BX/) (auto-downloaded). **If you use this data, cite the paper on that site.**
-   GoodReads data from [UCSD Book Graph](https://sites.google.com/eng.ucsd.edu/ucsdbookgraph/home) — the GoodReads books, works, authors, and *full interaction* files (**not** auto-downloaded - save GZip'd JSON files in `data`).  **If you use this data, cite the paper on that site.**

If all files are properly downloaded, `./dvc.sh status data/*.dvc` will show that all files are up to date, except for `loc-listings.dvc` which is 'always changed' (it may also display warnings about locked files).

```
$ ./dvc.sh status data/*.dvc
data/loc-listings.dvc:
        always changed
$
```

See [Data Model](../data/) for details on how each data source appears in the final data.
