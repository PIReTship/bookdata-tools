CREATE SCHEMA IF NOT EXISTS viaf;
DROP TABLE IF EXISTS viaf.marc_field CASCADE;
CREATE TABLE viaf.marc_field (
  rec_id INTEGER NOT NULL,
  fld_no INTEGER NOT NULL,
  tag VARCHAR NOT NULL,
  ind1 VARCHAR,
  ind2 VARCHAR,
  sf_code VARCHAR,
  contents VARCHAR
);

-- DROP TABLE IF EXISTS viaf_author CASCADE;
-- CREATE TABLE viaf_author (
--   viaf_au_id SERIAL PRIMARY KEY,
--   viaf_au_key VARCHAR NOT NULL UNIQUE
-- );

-- DROP TABLE IF EXISTS viaf_author_name CASCADE;
-- CREATE TABLE viaf_author_name (
--   viaf_au_id INTEGER NOT NULL,
--   viaf_au_name VARCHAR NULL,
--   viaf_au_name_type VARCHAR(1) NULL,
--   viaf_au_name_dates VARCHAR NULL,
--   viaf_au_name_source VARCHAR NULL
-- );

-- DROP TABLE IF EXISTS viaf_author_gender CASCADE;
-- CREATE TABLE viaf_author_gender (
--   viaf_au_id INTEGER NOT NULL,
--   viaf_au_gender VARCHAR NULL,
--   viaf_au_gender_start VARCHAR NULL,
--   viaf_au_gender_end VARCHAR NULL,
--   viaf_au_gender_source VARCHAR NULL
-- );
