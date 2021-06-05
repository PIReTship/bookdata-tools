table gender "cluster-genders.parquet"
table isbn_cluster "isbn-clusters.parquet"
table loc "../loc-mds/book-isbn-ids.parquet"
table bx_action "../bx/bx-cluster-actions.parquet"
table bx_rating "../bx/bx-cluster-ratings.parquet"

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
    SELECT 'LOC-MDS' as dataset, FILLNA(gender, 'no-book') AS gender, 'VIAF' AS source, COUNT(DISTINCT cluster) AS n_books
    FROM loc
    JOIN isbn_cluster USING (isbn_id)
    JOIN gender USING (cluster)
    GROUP BY gender
}

add-query {
    SELECT 'BX-I' as dataset, FILLNA(gender, 'no-book') AS gender, 'VIAF' AS source, COUNT(DISTINCT item) AS n_books
    FROM bx_action
    LEFT JOIN gender ON (item = cluster)
    GROUP BY FILLNA(gender, 'no-book')
}

save-results "gender-stats.csv.gz" $full_query
