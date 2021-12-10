table isbns "../book-links/all-isbns.parquet"
table books "book-isbns.parquet"

save-results "book-isbn-ids.parquet" {
    SELECT rec_id, isbn_id
    FROM books JOIN isbns USING (isbn)
}
