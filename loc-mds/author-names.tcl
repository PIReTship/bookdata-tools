# This file is not currently used
table fields "name-fields.parquet"

save-results "author-names.csv.gz" {
    SELECT rec_id, trim(contents) AS name
    FROM fields
    WHERE tag IN (100, 378) AND sf_code IN (97, 113)
}
