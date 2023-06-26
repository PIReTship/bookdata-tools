set gr_ix_plscript [file join [file dirname [info script]] "pl-${gr_interactions}.tcl"]
if {![file exists $gr_ix_plscript]} {
    error "goodreads: unknown interaction type $gr_interactions"
}

stage scan-book-info {
    bdcmd goodreads scan books ../data/goodreads/goodreads_books.json.gz
    dep ../src/cli/goodreads
    dep ../src/goodreads
    dep ../data/goodreads/goodreads_books.json.gz
    out gr-book-ids.parquet
    out gr-book-info.parquet
    out gr-book-authors.parquet
    out gr-book-series.parquet
}

stage scan-work-info {
    bdcmd goodreads scan works ../data/goodreads/goodreads_book_works.json.gz
    dep ../src/cli/goodreads
    dep ../src/goodreads
    dep ../data/goodreads/goodreads_book_works.json.gz
    out gr-work-info.parquet
}

stage scan-book-genres {
    bdcmd goodreads scan genres ../data/goodreads/goodreads_book_genres_initial.json.gz
    dep ../src/cli/goodreads
    dep ../src/goodreads
    dep ../data/goodreads/goodreads_book_genres_initial.json.gz
    out gr-book-genres.parquet
    out gr-genres.parquet
}

stage scan-author-info {
    bdcmd goodreads scan authors ../data/goodreads/goodreads_book_authors.json.gz
    dep ../src/cli/goodreads
    dep ../src/goodreads
    dep ../data/goodreads/goodreads_book_authors.json.gz
    out gr-author-info.parquet
}

stage book-isbn-ids {
    wdir ..
    bdcmd link-isbn-ids -o goodreads/book-isbn-ids.parquet -R book_id  -I isbn10 -I isbn13 -I asin goodreads/gr-book-ids.parquet
    dep src/cli/goodreads
    dep goodreads/gr-book-ids.parquet
    dep book-links/all-isbns.parquet
    out goodreads/book-isbn-ids.parquet
}

stage book-links {
    wdir ..
    bdcmd cluster extract-books -o goodreads/gr-book-link.parquet -n book_id --join-file goodreads/gr-book-ids.parquet --join-field work_id GR-B
    dep goodreads/gr-book-ids.parquet
    dep book-links/cluster-graph-nodes.parquet
    out goodreads/gr-book-link.parquet
}

stage work-gender {
    bdcmd goodreads work-gender
    dep ../src/cli/goodreads
    dep gr-book-link.parquet
    dep ../book-links/cluster-genders.parquet
    out gr-work-gender.parquet
}

source $gr_ix_plscript
