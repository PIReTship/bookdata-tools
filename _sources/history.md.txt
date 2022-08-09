# History

This page documents the release history of the Book Data Tools. Each numbered,
released version has a corresponding Git tag (e.g. `v2.0`).

## Book Data 2.1 (in progress)

Version 2.1 has a few updates but does not change existing data schemas.  It does
have improved book/author linking that increases coverage due to a revised and
corrected name parsing & normalization flow.

### Data Updates

-   Updated VIAF to May 1, 2022 dump
-   Updated OpenLibrary to March 29, 2022 dump
-   Added code to extract edition and work subjects
-   Updated docs for current extraction layout

### Logic Updates

-   Switched from DataFusion to [Polars](https://www.pola.rs/), to reduce volatility and improve
    maintainability.  This also involved a switch from Arrow to Arrow2, which seems to have cleaner
    code (and less custom logic needed for IO).  Python Polars code replaces the custom TCL driving
    DataFusion.
-   Improved name parsing
    -   Replaced `nom`-based name parser for {rust:fn}`~bookdata::cleaning::names::name_variants`
        with a new one written in [`peg`], that is both easier to read/maintain and more efficient.
    -   Corrected errors in name parser that emitted empty-string names for some authors.
    -   Added `clean_name` function, used across all name formatting, to normalize whitespace and
        punctuation in name records from any source.
    -   Added more tests for name parsing and normalization.
-   Fixed a bug in GoodReads integration, where we were not extracting ASINs.
-   Updated various Rust dependencies
-   Better progress reporting for data scans

[peg]: https://docs.rs/peg

## Book Data 2.0

This is the updated release of the Book Data Tools, using the same source data
as 1.0 but with DataFusion and Rust-based import logic, instead of PostgreSQL.
It is significantly easier to install and use.

## Book Data 1.0

The original release that used PostgreSQL. There were a couple of versions of
this for the RecSys and UMUAI papers; the tagged 1.0 release corresponds to the
data used for the UMUAI paper.
