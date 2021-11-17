table isbns "../book-links/all-isbns.parquet"
table editions "edition-isbns.parquet"

save-results "edition-isbn-ids.parquet" {
    SELECT edition, isbn_id
    FROM editions JOIN isbns USING (isbn)
}
