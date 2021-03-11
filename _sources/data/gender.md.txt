---
title: Book Author Gender
parent: Data Model
nav_order: 9
---

# Book Author Gender

We compute the author gender for book clusters using the integrated data set.

:::{warning}
See the [paper][] for important limitations and ethical considerations.
:::

[paper]: https://md.ekstrandom.net/pubs/bag-extended

## Import Steps

`integrate/author-info.dvc`
:   Run `integrate/author-info.sql` to compute book cluster author gender.

## Gender Integration

For each book cluster, the integration does the following:

1. Accumulate all names for the first author from OpenLibrary
2. Accumulate all names for the first/primary author from the Library of Congress
3. Obtain gender identities from all VIAF records matching an author name in this pool
4. Consolidate gender into a cluster author gender identity

The results of this are stored in `cluster_first_author_gender`, with the following fields:

`cluster`
:   The cluster identifier

`gender`
:   The gender identity of the first author, as one of the following:

    `no-loc-author`
    :   The book had no author records.

    `no-viaf-author`
    :   The author records did not match any VIAF records.

    `no-gender`
    :   The VIAF records had no gender information.

    `unknown`
    :   No gender informaiton available (for other reasons) or explicitly labeled as `unknown`.

    `male` or `female`
    :   The VIAF record unambiguously identifies this book's author as male or female.

    `ambiguous`
    :   The VIAF record(s) contained both male and female gender assertions.

The import process also creates some other intermediate views.

## Limitations

See the paper for a fuller discussion.  Some known limitations include:

- VIAF does not record non-binary gender identities.
- Recent versions of the OpenLibrary data contain VIAF identifiers for book authors, but we do not yet make use of this information.  When available, they should improve the reliability of book-author linking.
- GoodReads includes author names, but we do not yet use these for linking to gender records.
