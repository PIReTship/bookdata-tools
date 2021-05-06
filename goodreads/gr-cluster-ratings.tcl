table actions "gr-interactions.parquet"
table clusters "gr-book-link.parquet"

save-results "gr-cluster-ratings.parquet" {
    SELECT user_id AS user, cluster AS item, MEDIAN(rating) AS rating, COUNT(rating) AS nactions
    FROM actions
    JOIN clusters USING (book_id)
    WHERE rating IS NOT NULL
    GROUP BY user_id, cluster
}
