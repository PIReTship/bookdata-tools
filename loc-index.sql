-- Index MARC fields
CREATE INDEX loc_marc_field_rec_idx ON loc_marc_field (rec_id);
ALTER TABLE loc_marc_field ADD CONSTRAINT FOREIGN KEY (rec_id) REFERENCES loc_marc_record (rec_id);

-- Index ISBNs
CREATE MATERIALIZED VIEW loc_isbn
  AS SELECT rec_id, substring(contents from '^\s*([0-9A-Z]*)') AS isbn
  FROM loc_marc_field
  WHERE tag = '020';
CREATE INDEX loc_isbn_rec_idx ON loc_isbn (rec_id);
CREATE INDEX loc_isbn_isbn_idx ON loc_isbn (isbn);