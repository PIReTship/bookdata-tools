--- #step Index name MARC fields
CREATE INDEX IF NOT EXISTS name_marc_field_rec_idx ON locmds.name_marc_field (rec_id);

--- #step Index LOC author genders
DROP MATERIALIZED VIEW IF EXISTS locmds.author_gender;
CREATE MATERIALIZED VIEW locmds.author_gender
AS SELECT rec_id, contents AS gender
FROM locmds.name_marc_field
WHERE tag = '375' AND sf_code = 'a';
CREATE INDEX author_gender_rec_idx ON locmds.author_gender (rec_id);
ANALYZE locmds.author_gender;

--- #step Index LOC author names
DROP MATERIALIZED VIEW IF EXISTS locmds.author_name;
CREATE MATERIALIZED VIEW locmds.author_name
AS SELECT rec_id, trim(contents) AS name
FROM locmds.name_marc_field
WHERE tag IN ('100', '378') AND sf_code IN ('a', 'q');
CREATE INDEX author_name_rec_idx ON locmds.author_name (rec_id);
CREATE INDEX author_name_idx ON locmds.author_name (name);
ANALYZE locmds.author_name;
