table ratings "cleaned-ratings.csv"
table isbns "../book-links/all-isbns.parquet"
table clusters "../book-links/isbn-clusters.parquet"

save-results "bx-cluster-actions.parquet" {
    SELECT user, cluster AS item, COUNT(rating) AS nactions
    FROM ratings
    JOIN isbns USING (isbn)
    JOIN clusters USING (isbn_id)
    GROUP BY user, cluster
}
