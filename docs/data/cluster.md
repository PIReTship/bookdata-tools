---
title: Clusters
parent: Data Model
nav_order: 8
---

# Book Clusters
{: .no_toc}

For recommendation and analysis, we often want to look at *works* instead of individual books or
editions of those books.  The same material by the same author(s) may be reprinted in many different
editions, with different ISBNs, and sometimes separate ratings from the same user.

There are a variety of ways to deal with this.  GoodReads and OpenLibrary both have the concept of
a ‘work’ to group together related editions (the Library of Congress also has such a concept
internally in its BIBFRAME schema, but that data is not currently available for integration).

Other services, such as [ThingISBN](https://blog.librarything.com/thingology/2006/06/introducing-thingisbn/)
and OCLC's [xISBN](https://www.worldcat.org/affiliate/webservices/xisbn/app.jsp) both link together ISBNs:
given a query ISBN, they will return a list of ISBNs believed to be for the same book.

Using the book data sources here, we have implemented comparable functionality in a manner that
anyone can reproduce from public data.  We call the resulting equivalence sets ‘book clusters’.

## Clustering Algorithm

Our clustering algorithm begins by forming a bipartite graph of ISBNs and record identifiers.  We extract
records from the following:

- Library of Congress book records
- OpenLibrary editions. If the edition has an associated work, we use the work identifier instead of
  the book identifier.
- GoodReads books.  If the book has an associated work, we use the work identifier instead.

We convert each record identifier to a [book code](ids.html#book-codes) to avoid confusion between
different identifier types (and keep ID number reuse between data sets from colliding).

There is an edge from an ISBN to a record if that record reports the ISBN as one of its identifiers.

We then compute connected components on this graph, and treat each connected component as a single
‘book’ (what we call a *book cluster*).  The cluster is identified by the smallest book code of any
of the book records it comprises, but these cluster identifiers shouldn't be treated as meaningful.

The idea is that if two ISBNs appear together on a book record, that is evidence they are for the
same book; likewise, if two book records have the same ISBN, it is evidence they record the same book.
Pooling this evidence across all data sources maximizes the ability to detect book clusters.

The `isbn_cluster` table maps each ISBN to its associated cluster.  Individual data sources may also
have an `isbn_cluster` table (e.g. `gr.isbn_cluster`); that is the result of clustering ISBNs using
only the book records from that data source.  However, all clustered results such as rating tables
are based on the all-source book clusters.

## Known Problems

Some book sets have ISBNs, which cause them link together books that should not be clustered.
The Library of Congress identifies many of these ISBNs as set ISBNs, and we are examining the
prospect of using this to exclude them from clustering.
