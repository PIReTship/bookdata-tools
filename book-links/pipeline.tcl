stage collect-isbns {
    cmd python ../run.py --rust collect-isbns -o all-isbns.parquet all-isbns.toml
    dep ../src/cli/collect_isbns.rs
    dep all-isbns.toml
    dep ../loc-mds/book-isbns.parquet
    dep ../openlibrary/edition-isbns.parquet
    dep ../goodreads/gr-book-ids.parquet
    dep ../bx/cleaned-ratings.csv
    dep ../az2014/ratings.parquet
    dep ../az2018/ratings.parquet
    out all-isbns.parquet
}

stage cluster {
    cmd python run.py --rust cluster-books --save-graph book-links/book-graph.mp.zst
    wdir ..
    dep src/cli/cluster_books.rs
    dep src/graph/
    dep book-links/all-isbns.parquet
    dep loc-mds/book-ids.parquet
    dep loc-mds/book-isbn-ids.parquet
    dep openlibrary/editions.parquet
    dep openlibrary/edition-isbn-ids.parquet
    dep openlibrary/all-works.parquet
    dep openlibrary/edition-works.parquet
    dep goodreads/gr-book-ids.parquet
    dep goodreads/book-isbn-ids.parquet
    out book-links/book-graph.mp.zst
    out book-links/isbn-clusters.parquet
    out book-links/cluster-stats.parquet
    out book-links/cluster-graph-nodes.parquet
    out book-links/cluster-graph-edges.parquet
    out -metric book-links/cluster-metrics.json
}

stage cluster-ol-first-authors {
    cmd python run.py --rust cluster extract-authors -o book-links/cluster-ol-first-authors.parquet
    wdir ..
    dep src/cli/cluster
    dep book-links/isbn-clusters.parquet
    dep openlibrary/edition-isbn-ids.parquet
    dep openlibrary/edition-authors.parquet
    dep openlibrary/author-names.parquet
    out book-links/cluster-ol-first-authors.parquet
}

stage cluster-loc-first-authors {
    cmd python run.py --rust cluster extract-authors -o book-links/cluster-loc-first-authors.parquet
    wdir ..
    dep src/cli/cluster
    dep book-links/isbn-clusters.parquet
    dep loc-mds/book-isbn-ids.parquet
    dep loc-mds/book-authors.parquet
    out book-links/cluster-loc-first-authors.parquet
}

stage cluster-first-authors {
    cmd python run.py --rust cluster extract-authors -o book-links/cluster-first-authors.parquet
    wdir ..
    dep src/cli/cluster
    dep book-links/isbn-clusters.parquet
    dep openlibrary/edition-isbn-ids.parquet
    dep openlibrary/edition-authors.parquet
    dep openlibrary/author-names.parquet
    dep loc-mds/book-isbn-ids.parquet
    dep loc-mds/book-authors.parquet
    out book-links/cluster-first-authors.parquet
}

stage cluster-genders {
    cmd python run.py --rust cluster extract-author-gender -o book-links/cluster-genders.parquet
    wdir ..
    dep src/cli/cluster
    dep book-links/cluster-stats.parquet
    dep book-links/cluster-first-authors.parquet
    dep viaf/author-name-index.parquet
    dep viaf/author-genders.parquet
    out book-links/cluster-genders.parquet
}

stage gender-stats {
    cmd python run.py --rust integration-stats
    wdir ..
    dep config.toml
    dep src/cli/stats.rs
    dep book-links/cluster-genders.parquet
    dep book-links/isbn-clusters.parquet
    dep loc-mds/book-isbn-ids.parquet
    dep bx/bx-cluster-actions.parquet
    dep bx/bx-cluster-ratings.parquet
    dep az2014/az-cluster-ratings.parquet
    dep az2018/az-cluster-ratings.parquet
    dep goodreads/gr-cluster-actions.parquet
    dep goodreads/gr-cluster-ratings.parquet
    out book-links/gender-stats.csv
}

stage cluster-hashes {
    cmd python ../run.py --rust cluster hash -o cluster-hashes.parquet isbn-clusters.parquet
    dep ../src/cli/cluster/hash.rs
    dep isbn-clusters.parquet
    out cluster-hashes.parquet
}
