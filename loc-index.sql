-- Index MARC fields
CREATE INDEX loc_marc_field_rec_idx ON loc_marc_field (rec_id);;

-- Pull out control numbers
CREATE MATERIALIZED VIEW loc_marc_cn
  AS SELECT rec_id, trim(contents) AS control
  FROM loc_marc_field
  WHERE tag = '001';
CREATE INDEX loc_marc_cn_rec_idx ON loc_marc_cn (rec_id);
ANALYZE loc_marc_cn;
CREATE MATERIALIZED VIEW loc_lccn
  AS SELECT DISTINCT rec_id, trim(contents) AS lccn
  FROM loc_marc_field
  WHERE tag = '010' AND sf_code = 'a';
CREATE INDEX loc_lccn_rec_idx ON loc_lccn (rec_id);
ANALYZE loc_lccn;
CREATE VIEW loc_leader
  AS SELECT rec_id, contents AS leader
  FROM loc_marc_field
  WHERE tag = 'LDR';
CREATE VIEW loc_007_cat
  AS SELECT rec_id, LEFT(contents, 1) AS cat_type
  FROM loc_marc_field
  WHERE tag = '007';
CREATE VIEW loc_006_form
  AS SELECT rec_id, LEFT(contents, 1) AS form
  FROM loc_marc_field
  WHERE tag = '006';
CREATE VIEW loc_record_codes
  AS SELECT rec_id,
       SUBSTR(contents, 6, 1) AS status,
       SUBSTR(contents, 7, 1) AS rec_type,
       substr(CONTENTS, 8, 1) AS bib_level
  FROM loc_marc_field WHERE tag = 'LDR';
CREATE VIEW loc_gov_type
  AS SELECT rec_id, SUBSTR(pd.contents, 29, 1) AS gd_type
     FROM loc_record_codes
       LEFT JOIN (SELECT rec_id, contents FROM loc_marc_field WHERE tag = '008') pd USING (rec_id)
     WHERE rec_type IN ('a', 't');

CREATE MATERIALIZED VIEW loc_record_info
  AS SELECT rec_id, control AS marc_cn, lccn, status, rec_type, bib_level
  FROM loc_marc_cn
  LEFT JOIN loc_lccn USING (rec_id)
  JOIN loc_record_codes lrc USING (rec_id);
CREATE INDEX loc_record_rec_idx ON loc_record_info (rec_id);
CREATE INDEX loc_record_control_idx ON loc_record_info (marc_cn);
CREATE INDEX loc_record_lccn_idx ON loc_record_info (lccn);
ANALYZE loc_record_info;

-- A book is any text (MARC type a or t) that is not coded as a government document
CREATE MATERIALIZED VIEW loc_book
  AS SELECT rec_id, marc_cn, lccn, status, rec_type, bib_level
  FROM loc_record_info
  LEFT JOIN (SELECT rec_id, contents FROM loc_marc_field WHERE tag = '008') pd USING (rec_id)
  WHERE rec_type IN ('a', 't')
  AND (pd.contents IS NULL OR SUBSTRING(pd.contents, 29, 1) IN ('|', ' '));
CREATE INDEX loc_book_rec_idx ON loc_book (rec_id);
CREATE INDEX loc_book_control_idx ON loc_book (marc_cn);
CREATE INDEX loc_book_lccn_idx ON loc_book (lccn);
ANALYZE loc_book;

-- Index ISBNs
CREATE MATERIALIZED VIEW loc_isbn
  AS SELECT rec_id, replace(substring(contents from '^\s*(?:ISBN:?\s*)?([0-9A-Z-]*)'), '-', '') AS isbn
  FROM loc_book JOIN loc_marc_field USING (rec_id)
  WHERE tag = '020' AND sf_code = 'a' AND contents ~ '^\s*(?:ISBN:?\s*)?([0-9A-Z-]*)';
CREATE INDEX loc_isbn_rec_idx ON loc_isbn (rec_id);
CREATE INDEX loc_isbn_isbn_idx ON loc_isbn (isbn);

-- Extract authors
CREATE MATERIALIZED VIEW loc_author_name
  AS SELECT rec_id, regexp_replace(contents, '\W+$', '') AS name
  FROM loc_marc_field
  WHERE tag = '100' AND sf_code = 'a';
CREATE INDEX loc_author_name_rec_idx ON loc_author_name (rec_id);
CREATE INDEX loc_author_name_name_idx ON loc_author_name (name);