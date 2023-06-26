stage scan-authors {
    bdcmd openlib scan-authors ../data/openlib/ol_dump_authors.txt.gz
    dep ../src/cli/openlib.rs
    dep ../src/openlib/
    dep ../data/openlib/ol_dump_authors.txt.gz
    out authors.parquet
    out author-names.parquet
}
stage scan-works {
    bdcmd openlib scan-works ../data/openlib/ol_dump_works.txt.gz
    dep ../src/cli/openlib.rs
    dep ../src/openlib/
    dep ../data/openlib/ol_dump_works.txt.gz
    dep authors.parquet
    out works.parquet
    out work-authors.parquet
    out work-subjects.parquet
    out author-ids-after-works.parquet
}
stage scan-editions {
    bdcmd openlib scan-editions ../data/openlib/ol_dump_editions.txt.gz
    dep ../src/cli/openlib.rs
    dep ../src/openlib/
    dep ../data/openlib/ol_dump_editions.txt.gz
    dep authors.parquet
    dep works.parquet
    dep author-ids-after-works.parquet
    out editions.parquet
    out edition-works.parquet
    out edition-isbns.parquet
    out edition-authors.parquet
    out edition-subjects.parquet
    out all-works.parquet
    out all-authors.parquet
}
stage edition-isbn-ids {
    wdir ..
    bdcmd link-isbn-ids -R edition -o openlibrary/edition-isbn-ids.parquet openlibrary/edition-isbns.parquet
    dep openlibrary/edition-isbns.parquet
    dep book-links/all-isbns.parquet
    out openlibrary/edition-isbn-ids.parquet
}
stage work-clusters {
    wdir ..
    bdcmd cluster extract-books -n work_id -o openlibrary/work-clusters.parquet OL-W
    dep book-links/cluster-graph-nodes.parquet
    out openlibrary/work-clusters.parquet
}
