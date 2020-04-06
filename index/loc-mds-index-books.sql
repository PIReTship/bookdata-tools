--- #dep loc-mds-books
--- #dep loc-mds-extract-isbns
--- #table locmds.book_marc_cn
--- #table locmds.book_record_info
--- #table locmds.book
--- #step Index MARC fields
CREATE INDEX IF NOT EXISTS book_marc_field_rec_idx ON locmds.book_marc_field (rec_id);

--- #step Pull out control numbers
CREATE MATERIALIZED VIEW IF NOT EXISTS locmds.book_marc_cn
  AS SELECT rec_id, trim(contents) AS control
  FROM locmds.book_marc_field
  WHERE tag = '001'
  WITH NO DATA;
REFRESH MATERIALIZED VIEW locmds.book_marc_cn;
CREATE INDEX IF NOT EXISTS book_marc_cn_rec_idx ON locmds.book_marc_cn (rec_id);
ANALYZE locmds.book_marc_cn;
CREATE MATERIALIZED VIEW IF NOT EXISTS locmds.book_lccn
  AS SELECT DISTINCT rec_id, trim(contents) AS lccn
  FROM locmds.book_marc_field
  WHERE tag = '010' AND sf_code = 'a'
  WITH NO DATA;
REFRESH MATERIALIZED VIEW locmds.book_lccn;
CREATE INDEX IF NOT EXISTS book_lccn_rec_idx ON locmds.book_lccn (rec_id);
ANALYZE locmds.book_lccn;
DROP VIEW IF EXISTS locmds.book_leader;
CREATE VIEW locmds.book_leader
  AS SELECT rec_id, contents AS leader
  FROM locmds.book_marc_field
  WHERE tag = 'LDR';
DROP VIEW IF EXISTS locmds.book_007_cat;
CREATE VIEW locmds.book_007_cat
  AS SELECT rec_id, LEFT(contents, 1) AS cat_type
  FROM locmds.book_marc_field
  WHERE tag = '007';
DROP VIEW IF EXISTS locmds.book_006_form;
CREATE VIEW locmds.book_006_form
  AS SELECT rec_id, LEFT(contents, 1) AS form
  FROM locmds.book_marc_field
  WHERE tag = '006';
DROP VIEW IF EXISTS locmds.book_record_code CASCADE;
CREATE VIEW locmds.book_record_code
  AS SELECT rec_id,
       SUBSTR(contents, 6, 1) AS status,
       SUBSTR(contents, 7, 1) AS rec_type,
       substr(CONTENTS, 8, 1) AS bib_level
  FROM locmds.book_marc_field WHERE tag = 'LDR';
DROP VIEW IF EXISTS locmds.book_gov_type;
CREATE VIEW locmds.book_gov_type
  AS SELECT rec_id, SUBSTR(pd.contents, 29, 1) AS gd_type
     FROM locmds.book_record_code
       LEFT JOIN (SELECT rec_id, contents FROM locmds.book_marc_field WHERE tag = '008') pd USING (rec_id)
     WHERE rec_type IN ('a', 't');

CREATE MATERIALIZED VIEW IF NOT EXISTS locmds.book_record_info
  AS SELECT rec_id, control AS marc_cn, lccn, status, rec_type, bib_level
  FROM locmds.book_marc_cn
  LEFT JOIN locmds.book_lccn USING (rec_id)
  JOIN locmds.book_record_code lrc USING (rec_id)
  WITH NO DATA;
REFRESH MATERIALIZED VIEW locmds.book_record_info;
CREATE INDEX IF NOT EXISTS book_record_rec_idx ON locmds.book_record_info (rec_id);
CREATE INDEX IF NOT EXISTS book_record_control_idx ON locmds.book_record_info (marc_cn);
CREATE INDEX IF NOT EXISTS book_record_lccn_idx ON locmds.book_record_info (lccn);
ANALYZE locmds.book_record_info;

-- A book is any text (MARC type a or t) that is not coded as a government document
CREATE MATERIALIZED VIEW IF NOT EXISTS locmds.book
  AS SELECT rec_id, marc_cn, lccn, status, rec_type, bib_level
  FROM locmds.book_record_info
  LEFT JOIN (SELECT rec_id, contents FROM locmds.book_marc_field WHERE tag = '008') pd USING (rec_id)
  WHERE rec_type IN ('a', 't')
  AND (pd.contents IS NULL OR SUBSTRING(pd.contents, 29, 1) IN ('|', ' '))
  WITH NO DATA;
REFRESH MATERIALIZED VIEW locmds.book;
CREATE INDEX IF NOT EXISTS book_rec_idx ON locmds.book (rec_id);
CREATE INDEX IF NOT EXISTS book_control_idx ON locmds.book (marc_cn);
CREATE INDEX IF NOT EXISTS book_lccn_idx ON locmds.book (lccn);
ANALYZE locmds.book;

--- #step Index and link ISBNs
INSERT INTO isbn_id (isbn)
  WITH isbns AS (SELECT DISTINCT isbn
                 FROM locmds.book_extracted_isbn
                 WHERE isbn IS NOT NULL)
  SELECT isbn FROM isbns
  WHERE isbn NOT IN (SELECT isbn FROM isbn_id);
ANALYZE isbn_id;

CREATE MATERIALIZED VIEW locmds.book_rec_isbn
  AS SELECT DISTINCT rec_id, isbn_id
     FROM locmds.book JOIN locmds.book_extracted_isbn USING (rec_id) JOIN isbn_id USING (isbn)
     WHERE isbn IS NOT NULl AND char_length(isbn) IN (10,13);
CREATE INDEX IF NOT EXISTS book_rec_isbn_rec_idx ON locmds.book_rec_isbn (rec_id);
CREATE INDEX IF NOT EXISTS book_rec_isbn_isbn_idx ON locmds.book_rec_isbn (isbn_id);
ANALYZE locmds.book_rec_isbn;
