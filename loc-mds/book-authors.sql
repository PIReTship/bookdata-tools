SELECT rec_id, TRIM(contents) AS name
FROM fields
WHERE tag = 100 AND sf_code = 97
LIMIT 100;
