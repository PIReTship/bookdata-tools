# This file is not currently used.
table fields "name-fields.parquet"

save-results "author-genders.csv.gz" {
    SELECT rec_id, LOWER(TRIM(contents)) AS gender
    FROM fields
    WHERE tag = 375 AND sf_code = ASCII('a');
}
