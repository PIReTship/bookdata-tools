table fields "viaf.parquet"

save-results "author-names.csv.gz" {
    SELECT rec_id, CHR(ind1) AS ind1, CHR(ind2) AS ind2, CHR(sf_code), TRIM(contents) AS name
    FROM fields
    WHERE tag = 700 AND sf_code = ASCII('a')
}
