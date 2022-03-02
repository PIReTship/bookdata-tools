---
title: Source Data
---

# Downloading Source Data

These import tools will integrate several data sets. Some of them are auto-downloaded, but others you will
need to download yourself and save in the `data` directory.  The data sources are:

-   [Library of Congress MDSConnect Open MARC Records](https://www.loc.gov/cds/products/MDSConnect-books_all.html) (auto-downloaded).
-   [LoC MDSConnect Name Authorities](https://www.loc.gov/cds/products/MDSConnect-name_authorities.html) (auto-downloaded).
-   [Virtual Internet Authority File](http://viaf.org/viaf/data/) MARC 21 XML data (auto-downloaded; download is very slow).
-   [OpenLibrary Dump](https://openlibrary.org/developers/dumps) (auto-downloaded).
-   [Amazon Ratings](http://jmcauley.ucsd.edu/data/amazon/) 'ratings only' data for _Books_ (**not** auto-downloaded — save CSV file in `data/az2014`).  **If you use this data, cite the paper on that site.**
-   [BookCrossing](http://www2.informatik.uni-freiburg.de/~cziegler/BX/) (auto-downloaded). **If you use this data, cite the paper on that site.**
-   GoodReads data from [UCSD Book Graph](https://sites.google.com/eng.ucsd.edu/ucsdbookgraph/home) — the GoodReads books, works, authors, series, and *full interaction* files (**not** auto-downloaded - save GZip'd JSON files in `data/goodreads`).  **If you use this data, cite the paper on that site.**

If all files are properly downloaded, `dvc status -R data` will show that all files are up to date (it may also display warnings about locked files).

See [Data Model](../data/) for details on how each data source appears in the final data.
