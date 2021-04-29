table fields "book-fields.parquet"

write "book-authors.parquet" {
    SELECT rec_id, TRIM(contents) AS author_name
    FROM fields
    WHERE tag = 100 AND sf_code = 97
}
