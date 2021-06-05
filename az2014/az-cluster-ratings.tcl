table ratings "ratings.parquet"
table isbns "../book-links/all-isbns.parquet"
table clusters "../book-links/isbn-clusters.parquet"

save-results "az-cluster-ratings.parquet" {
    SELECT user, cluster AS item, MEDIAN(rating) AS rating, COUNT(rating) AS nratings
    FROM ratings
    JOIN isbns ON (asin = isbn)
    JOIN clusters USING (isbn_id)
    GROUP BY user, cluster
}
