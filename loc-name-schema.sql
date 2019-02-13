DROP TABLE IF EXISTS locmds_name_marc_field CASCADE;
CREATE TABLE locmds_name_marc_field (
  rec_id INTEGER NOT NULL,
  fld_no INTEGER NOT NULL,
  tag VARCHAR NOT NULL,
  ind1 VARCHAR,
  ind2 VARCHAR,
  sf_code VARCHAR,
  contents VARCHAR
);
