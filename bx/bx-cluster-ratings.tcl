table ratings "cleaned-ratings.csv"
table isbns "../book-links/all-isbns.parquet"
table clusters "../book-links/isbn-clusters.parquet"

save-results "bx-cluster-ratings.csv.gz" {
    SELECT user, cluster AS item, MEDIAN(rating), COUNT(rating) AS nratings
    FROM ratings
    JOIN isbns USING (isbn)
    JOIN clusters USING (isbn_id)
    WHERE rating > 0
    GROUP BY user, cluster
}
