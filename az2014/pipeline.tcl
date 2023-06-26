stage scan-ratings {
    cmd python ../run.py --rust amazon scan-ratings -o ratings.parquet ../data/az2014/ratings_Books.csv
    dep ../src/amazon.rs
    dep ../src/cli/amazon/
    dep ../data/az2014/ratings_Books.csv
    out ratings.parquet
}

stage cluster-ratings {
    cmd python run.py --rust amazon cluster-ratings -o az2014/az-cluster-ratings.parquet az2014/ratings.parquet
    wdir ..
    dep src/cli/amazon
    dep az2014/ratings.parquet
    dep book-links/isbn-clusters.parquet
    out az2014/az-cluster-ratings.parquet
}

stage cluster-ratings-5core {
    cmd python ../run.py --rust kcore -o az-cluster-ratings-5core.parquet az-cluster-ratings.parquet
    dep az-cluster-ratings.parquet
    dep ../src/cli/kcore.rs
    out az-cluster-ratings-5core.parquet
}
