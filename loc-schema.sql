CREATE TABLE loc_marc_records ( 
  rec_id INTEGER PRIMARY KEY,
  title VARCHAR,
  marc TEXT
);
CREATE TABLE loc_marc_fields (
  rec_id INTEGER NOT NULL,
  fid INTEGER NOT NULL,
  field VARCHAR NOT NULL,
  ind1 VARCHAR,
  ind2 VARCHAR,
  subfield VARCHAR,
  field_data VARCHAR
);
