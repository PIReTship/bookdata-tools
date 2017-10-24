DROP TABLE IF EXISTS viaf_author CASCADE;
CREATE TABLE viaf_author (
  viaf_au_id BIGINT PRIMARY KEY
);

DROP TABLE IF EXISTS viaf_author CASCADE;
CREATE TABLE viaf_author_name (
  viaf_au_id BIGINT NOT NULL,
  viaf_au_name VARCHAR NOT NULL,
  viaf_au_name_type VARCHAR(1) NULL,
  viaf_au_name_dates VARCHAR NULL,
  viaf_au_name_source VARCHAR NULL
);

DROP TABLE IF EXISTS viaf_author CASCADE;
CREATE TABLE viaf_author_gender (
  viaf_au_id BIGINT NOT NULL,
  viaf_au_gender VARCHAR NOT NULL,
  viaf_au_gender_start VARCHAR NULL,
  viaf_au_gender_end VARCHAR NULL,
  viaf_au_gender_source VARCHAR NULL
);