table actions "gr-interactions.parquet"
table books "book-isbn-ids.parquet"
table clusters "../book-links/isbn-clusters.parquet"

save-results "gr-cluster-ratings.parquet" {
    SELECT user_id AS user, cluster AS item, MEDIAN(rating) AS rating, COUNT(rating) AS nactions
    FROM books
    JOIN clusters USING (isbn_id)
    JOIN actions USING (book_id)
    WHERE rating IS NOT NULL
    GROUP BY user_id, cluster
}
