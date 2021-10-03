table nodes "cluster-graph-nodes.parquet"
table edges "cluster-graph-edges.parquet"

save-results "large-cluster-nodes.csv" {
    SELECT book_code AS node, node_type AS class, label
    FROM nodes
    WHERE cluster = 100000283
}

save-results "large-cluster-edges.csv" {
    SELECT src AS Source, dst AS Target
    FROM edges
    JOIN nodes ON src = book_code
    WHERE cluster = 100000283
}
