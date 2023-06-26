stage scan-books {
    bdcmd scan-marc --book-mode --glob {"../data/loc-books/BooksAll.2016*.xml.gz"}
    dep ../src/cli/scan_marc.rs
    dep ../src/marc
    dep ../data/loc-books
    out book-fields.parquet
    out book-ids.parquet
    out book-isbns.parquet
    out book-authors.parquet
}

stage scan-names {
    bdcmd scan-marc --glob {"../data/loc-names/Names.2016*.xml.gz"} -o name-fields.parquet
    dep ../src/cli/scan_marc.rs
    dep ../src/marc
    dep ../data/loc-names
    out name-fields.parquet
}

stage book-isbn-ids {
    wdir ..
    bdcmd link-isbn-ids -R rec_id -o loc-mds/book-isbn-ids.parquet loc-mds/book-isbns.parquet
    dep loc-mds/book-isbns.parquet
    dep book-links/all-isbns.parquet
    out loc-mds/book-isbn-ids.parquet
}
