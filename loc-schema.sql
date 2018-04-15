DROP TABLE IF EXISTS loc_marc_record CASCADE;
CREATE TABLE loc_marc_record ( 
  rec_id SERIAL PRIMARY KEY,
  lccn VARCHAR UNIQUE
);
DROP TABLE IF EXISTS loc_marc_field CASCADE;
CREATE TABLE loc_marc_field (
  rec_id INTEGER NOT NULL,
  fld_no INTEGER NOT NULL,
  tag VARCHAR NOT NULL,
  ind1 VARCHAR,
  ind2 VARCHAR,
  sf_code VARCHAR,
  contents VARCHAR
);
