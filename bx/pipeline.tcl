stage clean-ratings {
    cmd python ../run.py --rust bx extract ../data/BX-CSV-Dump.zip cleaned-ratings.csv
    dep ../src/cli/bx
    dep ../data/BX-CSV-Dump.zip
    out cleaned-ratings.csv
}

stage cluster-ratings {
    cmd python ../run.py --rust bx cluster-actions --ratings -o bx-cluster-ratings.parquet
    dep ../src/cli/bx
    dep cleaned-ratings.csv
    dep ../book-links/isbn-clusters.parquet
    out bx-cluster-ratings.parquet
}

stage cluster-actions {
    cmd python ../run.py --rust bx cluster-actions --add-actions -o bx-cluster-actions.parquet
    dep ../src/cli/bx
    dep cleaned-ratings.csv
    dep ../book-links/isbn-clusters.parquet
    out bx-cluster-actions.parquet
}
