-- Index MARC fields
CREATE INDEX loc_marc_field_rec_idx ON loc_marc_field (rec_id);

-- Pull out control numbers
CREATE MATERIALIZED VIEW loc_marc_cn
  AS SELECT rec_id, trim(contents) AS control
  FROM loc_marc_field
  WHERE tag = '001';
CREATE MATERIALIZED VIEW loc_lccn
  AS SELECT rec_id, trim(contents) AS lccn
  FROM loc_marc_field
  WHERE tag = '010';
CREATE MATERIALIZED VIEW loc_isbn
  AS SELECT rec_id,

-- Index ISBNs
CREATE MATERIALIZED VIEW loc_isbn
  AS SELECT rec_id, substring(contents from '^\s*([0-9A-Z]*)') AS isbn
  FROM loc_marc_field
  WHERE tag = '020';
CREATE INDEX loc_isbn_rec_idx ON loc_isbn (rec_id);
CREATE INDEX loc_isbn_isbn_idx ON loc_isbn (isbn);