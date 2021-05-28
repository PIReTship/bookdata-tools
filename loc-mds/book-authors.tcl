table fields "book-fields.parquet"

save-results "book-authors.parquet" {
    SELECT rec_id, NORM_UNICODE(REGEXP_REPLACE(TRIM(contents), '\W+$', '')) AS author_name
    FROM fields
    WHERE tag = 100 AND sf_code = 97 AND contents IS NOT NULL
}
