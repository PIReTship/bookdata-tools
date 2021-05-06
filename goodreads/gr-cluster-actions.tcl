table actions "gr-interactions.parquet"
table books "book-isbn-ids.parquet"
table clusters "../book-links/isbn-clusters.parquet"

save-results "gr-cluster-actions.parquet" {
    SELECT user_id AS user, item, COUNT(user_id) AS nactions
    FROM (SELECT book_id, cluster AS item FROM books JOIN clusters USING (isbn_id))
    JOIN actions USING (book_id)
    GROUP BY user_id, item
}
