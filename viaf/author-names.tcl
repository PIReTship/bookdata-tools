table fields "viaf.parquet"

save-results "author-names.csv.gz" {
    SELECT rec_id, ind1, TRIM(contents) AS name
    FROM fields
    WHERE tag = 700 AND sf_code = ASCII('a')
}
