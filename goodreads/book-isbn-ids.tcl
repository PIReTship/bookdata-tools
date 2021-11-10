table isbns "../book-links/all-isbns.parquet"
table books "gr-book-ids.parquet"

save-results "book-isbn-ids.parquet" {
    SELECT book_id, isbn_id
    FROM books JOIN isbns ON (isbn10 = isbn)
    WHERE isbn10 IS NOT NULL
    UNION ALL
    SELECT book_id, isbn_id
    FROM books JOIN isbns ON (isbn13 = isbn)
    WHERE isbn13 IS NOT NULL
    UNION ALL
    SELECT book_id, isbn_id
    FROM books JOIN isbns ON (isbn13 = isbn)
    WHERE asin IS NOT NULL
}
