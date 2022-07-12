---
title: Clusters
parent: Data Model
---

(cluster)=
# Book Clusters

:::{index} cluster
:::

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

Our clustering algorithm begins by forming an undirected graph of record identifiers.  We extract
records from the following:

- Library of Congress book records, with edges from records to ISBNs recorded for that record.
- OpenLibrary editions, with edges from editions to ISBNs recorded for that edition.
- OpenLibrary works, with edges from works to editions.
- GoodReads books, with edges from books to ISBNs recorded for that book.
- GoodReads works, with edges from works to books.

We then compute the connected components on this graph, and treat each connected component as a single
‘book’ (what we call a *book cluster*).

The idea is that if two ISBNs appear together on a book record, that is evidence they are for the
same book; likewise, if two book records have the same ISBN, it is evidence they record the same book.
Pooling this evidence across all data sources maximizes the ability to detect book clusters.

The `isbn_cluster` table maps each ISBN to its associated cluster.  Individual data sources may also
have an `isbn_cluster` table (e.g. `gr.isbn_cluster`); that is the result of clustering ISBNs using
only the book records from that data source.  However, all clustered results such as rating tables
are based on the all-source book clusters.

## Known Problems

There are a few known problems with the ISBN clustering:

- Publishers occasionally reuse ISBNs.  They aren't supposed to do this, but they do.  This results
  in unrelated books having the same ISBN.  This will cause a problem for any ISBN-based linking
  between books and ratings, not just the book clustering.  We don't yet have a good way to identify
  these ISBNs.

- Some book sets have ISBNs, which cause them link together books that should not be clustered.
  The Library of Congress identifies many of these ISBNs as set ISBNs, and we are examining the
  prospect of using this to exclude them from informing clustering decisions.

If you only need e.g. the GoodReads data, we recommend that you *not* cluster it for the purpose of
ratings, and only use clusters to link to out-of-GR book or author data.  We are open to adding
additional tables that facilitate linking GoodReads works directly to other tables.

## Cluster Link Tables

:::{file} book-links/isbn-clusters.parquet

This file maps ISBN IDs to book clusters, enabling the various other book identifiers from other
data sources to be mapped to clusters, since everything resolves to ISBN IDs.
:::

### Graph Tables

We also export the book identifier graph used for clustering to support further analysis.

:::{file} book-links/cluster-graph-nodes.parquet

The table of nodes (with attributes) from the graph used for book clustering.
:::

:::{file} book-links/cluster-graph-edges.parquet

The table of edges rom the book clustering graph.
:::

:::{file} book-links/book-graph.mp.zst

This is a serialization of the actual graph itself, using `rmp-serde` to serialize the
Petgraph structure with MsgPack and compressing ith with ZStandard.  This is unlikely to
be usable outside of the Rust codebase, whereas the node and edge tables could be loaded
into something like `igraph` for further analysis.
:::

## Cluster Information Tables

With the clusters, we then extract additional information from other tables.

:::{file} book-links/cluster-first-authors.parquet

All available first-author records for each cluster, to support linking with VIAF.
:::

:::{file} book-links/cluster-hashes.parquet

The MD5 checksums of the sorted sequence of ISBNs for each cluster, along with a
`dcode` that is the least-significant bit of the checksum.
:::

:::{file} book-links/cluster-stats.parquet

Statistics for each cluster, useful for auditing and debugging.
:::
