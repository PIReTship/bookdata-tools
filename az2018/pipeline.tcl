stage scan-ratings {
    bdcmd amazon scan-ratings -o ratings.parquet --swap-id-columns ../data/az2018/Books.csv
    dep ../src/amazon.rs
    dep ../src/cli/amazon/
    dep ../data/az2018/Books.csv
    out ratings.parquet
}

stage cluster-ratings {
    wdir ..
    bdcmd amazon cluster-ratings -o az2018/az-cluster-ratings.parquet az2018/ratings.parquet
    dep src/cli/amazon
    dep az2018/ratings.parquet
    dep book-links/isbn-clusters.parquet
    out az2018/az-cluster-ratings.parquet
}

stage cluster-ratings-5core {
    bdcmd kcore -o az-cluster-ratings-5core.parquet az-cluster-ratings.parquet
    dep az-cluster-ratings.parquet
    dep ../src/cli/kcore.rs
    out az-cluster-ratings-5core.parquet
}
