stage scan-authors {
    cmd python ../run.py --rust scan-marc -L -o viaf.parquet ../data/viaf-clusters-marc21.xml.gz
    dep ../src/cli/scan_marc.rs
    dep ../src/marc
    dep ../data/viaf-clusters-marc21.xml.gz
    out viaf.parquet
}

stage author-genders {
    cmd python ../run.py --rust filter-marc --tag=375 --subfield=a --trim --lower -n gender -o author-genders.parquet viaf.parquet
    dep ../src/cli/filter_marc.rs
    dep viaf.parquet
    out author-genders.parquet
}

stage index-names {
    cmd python ../run.py --rust index-names --marc-authorities viaf.parquet author-name-index.parquet
    dep ../src/cli/index_names.rs
    dep ../src/cleaning/names
    dep viaf.parquet
    out author-name-index.parquet
    out author-name-index.csv.gz
}
