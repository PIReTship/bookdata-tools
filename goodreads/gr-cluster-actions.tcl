table actions "gr-interactions.parquet"
table clusters "gr-book-link.parquet"

save-results "gr-cluster-actions.parquet" {
    SELECT user_id AS user, cluster AS item, COUNT(user_id) AS nactions
    FROM actions
    JOIN clusters USING (book_id)
    GROUP BY user_id, cluster
}
