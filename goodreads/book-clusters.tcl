table clusters "../book-links/cluster-codes.parquet"
table book_ids "gr-book-ids.parquet"

save-results "gr-book-link.parquet" {
    SELECT book_id, work_id, cluster
    FROM book_ids
    JOIN (SELECT gr_book_from_code(book_code) AS grbid, cluster
          FROM clusters
          WHERE code_is_gr_book(book_code)) bc ON book_id = grbid
}
