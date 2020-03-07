--- #dep common-schema
--- #table locmds.book_marc_field
--- #table locmds.name_marc_field

CREATE SCHEMA IF NOT EXISTS locmds;

DROP TABLE IF EXISTS locmds.book_marc_field CASCADE;
CREATE TABLE locmds.book_marc_field (
  rec_id INTEGER NOT NULL,
  fld_no INTEGER NOT NULL,
  tag VARCHAR NOT NULL,
  ind1 VARCHAR,
  ind2 VARCHAR,
  sf_code VARCHAR,
  contents VARCHAR
);

DROP TABLE IF EXISTS locmds.name_marc_field CASCADE;
CREATE TABLE locmds.name_marc_field (
  rec_id INTEGER NOT NULL,
  fld_no INTEGER NOT NULL,
  tag VARCHAR NOT NULL,
  ind1 VARCHAR,
  ind2 VARCHAR,
  sf_code VARCHAR,
  contents VARCHAR
);
