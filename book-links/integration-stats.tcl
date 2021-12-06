table gender "cluster-genders.parquet"
table isbn_cluster "isbn-clusters.parquet"
table loc "../loc-mds/book-isbn-ids.parquet"
table bx_action "../bx/bx-cluster-actions.parquet"
table bx_rating "../bx/bx-cluster-ratings.parquet"
table az14_rating "../az2014/az-cluster-ratings.parquet"
table az18_rating "../az2018/az-cluster-ratings.parquet"
table gr_rating "../goodreads/gr-cluster-ratings.parquet"
table gr_action "../goodreads/gr-cluster-actions.parquet"

table nodes "cluster-graph-nodes.parquet"

query {
    SELECT node_type, COUNT(cluster)
    FROM nodes
    GROUP BY node_type
    ORDER BY node_type
}

set full_query ""

proc add-query {query} {
    global full_query
    if {$full_query eq ""} {
        set full_query $query
    } else {
        set full_query "$full_query UNION ALL $query"
    }
}

add-query {
    SELECT 'LOC-MDS' as dataset, gender, COUNT(DISTINCT cluster) AS n_books, COUNT(NULL) AS n_actions
    FROM loc
    JOIN isbn_cluster USING (isbn_id)
    JOIN gender USING (cluster)
    GROUP BY gender
}

add-query {
    SELECT 'BX-I' as dataset, gender, COUNT(DISTINCT item) AS n_books, COUNT(item) AS n_actions
    FROM bx_action
    JOIN gender ON (item = cluster)
    GROUP BY gender
}

add-query {
    SELECT 'BX-E' as dataset, gender, COUNT(DISTINCT item) AS n_books, COUNT(item) AS n_actions
    FROM bx_rating
    JOIN gender ON (item = cluster)
    GROUP BY gender
}

add-query {
    SELECT 'AZ14' as dataset, gender, COUNT(DISTINCT item) AS n_books, COUNT(item) AS n_actions
    FROM az14_rating
    JOIN gender ON (item = cluster)
    GROUP BY gender
}

add-query {
    SELECT 'AZ18' as dataset, gender, COUNT(DISTINCT item) AS n_books, COUNT(item) AS n_actions
    FROM az18_rating
    JOIN gender ON (item = cluster)
    GROUP BY gender
}

add-query {
    SELECT 'GR-I' as dataset, gender, COUNT(DISTINCT item) AS n_books, COUNT(item) AS n_actions
    FROM gr_action
    JOIN gender ON (item = cluster)
    GROUP BY gender
}

add-query {
    SELECT 'GR-E' as dataset, gender, COUNT(DISTINCT item) AS n_books, COUNT(item) AS n_actions
    FROM gr_rating
    JOIN gender ON (item = cluster)
    GROUP BY gender
}

save-results "gender-stats.csv" $full_query
