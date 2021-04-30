table fields "viaf.parquet"

save-results "author-names.csv.gz" {
    SELECT rec_id, LOWER(TRIM(contents)) AS gender
    FROM fields
    WHERE tag = 375 AND sf_code = ASCII('a')
}

