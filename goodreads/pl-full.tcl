# Full interaction pipeline
stage scan-interactions {
    cmd python ../run.py --rust goodreads scan interactions ../data/goodreads/goodreads_interactions.json.gz
    dep ../src/cli/goodreads
    dep ../src/goodreads
    dep ../data/goodreads/goodreads_interactions.json.gz
    out gr-interactions.parquet
    out gr-users.parquet
}

stage cluster-actions {
    cmd python run.py --rust goodreads cluster-interactions --add-actions -o goodreads/full/gr-cluster-actions.parquet
    wdir ..
    dep src/cli/goodreads/cluster.rs
    dep goodreads/full/gr-interactions.parquet
    dep goodreads/gr-book-link.parquet
    out goodreads/full/gr-cluster-actions.parquet
}

stage cluster-ratings {
    cmd python run.py --rust goodreads cluster-interactions --ratings -o goodreads/full/gr-cluster-ratings.parquet
    wdir ..
    dep src/cli/goodreads/cluster.rs
    dep goodreads/full/gr-interactions.parquet
    dep goodreads/gr-book-link.parquet
    out goodreads/full/gr-cluster-ratings.parquet
}

stage cluster-ratings-5core {
    cmd python ../run.py --rust kcore -o gr-cluster-ratings-5core.parquet gr-cluster-ratings.parquet
    dep gr-cluster-ratings.parquet
    dep ../src/cli/kcore.rs
    out gr-cluster-ratings-5core.parquet
}

stage cluster-actions-5core {
    cmd python ../run.py --rust kcore -o gr-cluster-actions-5core.parquet gr-cluster-actions.parquet
    dep gr-cluster-actions.parquet
    dep ../src/cli/kcore.rs
    out gr-cluster-actions-5core.parquet
}

stage work-actions {
    cmd python run.py --rust goodreads cluster-interactions --add-actions --native-works -o goodreads/full/gr-work-actions.parquet
    wdir ..
    dep src/cli/goodreads/cluster.rs
    dep goodreads/full/gr-interactions.parquet
    dep goodreads/gr-book-link.parquet
    out goodreads/full/gr-work-actions.parquet
}

stage work-ratings {
    cmd python run.py --rust goodreads cluster-interactions --ratings --native-works -o goodreads/full/gr-work-ratings.parquet
    wdir ..
    dep src/cli/goodreads/cluster.rs
    dep goodreads/full/gr-interactions.parquet
    dep goodreads/gr-book-link.parquet
    out goodreads/full/gr-work-ratings.parquet
}

stage work-ratings-5core {
    cmd python ../run.py --rust kcore -o gr-work-ratings-5core.parquet gr-work-ratings.parquet
    dep gr-work-ratings.parquet
    dep ../src/cli/kcore.rs
    out gr-work-ratings-5core.parquet
}

stage work-actions-5core {
    cmd python ../run.py --rust kcore -o gr-work-actions-5core.parquet gr-work-actions.parquet
    dep gr-work-actions.parquet
    dep ../src/cli/kcore.rs
    out gr-work-actions-5core.parquet
}

stage work-ratings-2015-100-10core {
    cmd python ../run.py --rust kcore --user-k 10 --item-k 100 --year 2015 -o gr-work-ratings-2015-100-10core.parquet gr-work-ratings.parquet
    dep gr-work-ratings.parquet
    dep ../src/cli/kcore.rs
    out gr-work-ratings-2015-100-10core.parquet
}

stage work-actions-2015-100-10core {
    cmd python ../run.py --rust kcore --user-k 10 --item-k 100 --year 2015 -o gr-work-actions-2015-100-10core.parquet gr-work-actions.parquet
    dep gr-work-actions.parquet
    dep ../src/cli/kcore.rs
    out gr-work-actions-2015-100-10core.parquet
}
