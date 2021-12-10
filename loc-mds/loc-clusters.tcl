table clusters "../book-links/cluster-graph-nodes.parquet"

save-results loc-clusters.parquet {
    SELECT cluster, COUNT(book_code) AS n_recs
    FROM clusters
    WHERE code_is_loc_rec(book_code)
    GROUP BY cluster
}
