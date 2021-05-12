table fields "book-fields.parquet"

save-results "book-authors.parquet" {
    SELECT rec_id, TRIM(contents) AS author_name
    FROM fields
    WHERE tag = 100 AND sf_code = 97 AND contents IS NOT NULL
}
