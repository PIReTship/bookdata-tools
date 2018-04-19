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
DROP MATERIALIZED VIEW IF EXISTS loc_isbn;
CREATE MATERIALIZED VIEW loc_isbn
  AS SELECT rec_id, replace(substring(contents from '^\s*(?:ISBN:?\s*)?([0-9X-]+)'), '-', '') AS isbn
  FROM loc_book JOIN loc_marc_field USING (rec_id)
  WHERE tag = '020' AND sf_code = 'a' AND contents ~ '^\s*(?:ISBN:?\s*)?([0-9X-]+)';
CREATE INDEX loc_isbn_rec_idx ON loc_isbn (rec_id);
CREATE INDEX loc_isbn_isbn_idx ON loc_isbn (isbn);

-- Construct ISBN peers
DROP MATERIALIZED VIEW IF EXISTS loc_isbn_peer;
CREATE MATERIALIZED VIEW loc_isbn_peer
  AS WITH RECURSIVE
      peer (isbn1, isbn2) AS (SELECT li1.isbn, li2.isbn
                              FROM loc_isbn li1
                                JOIN loc_isbn li2 USING (rec_id)
                              UNION DISTINCT
                              SELECT p.isbn1, li2.isbn
                              FROM peer p
                                JOIN loc_isbn li1 ON (p.isbn1 = li1.isbn)
                                JOIN loc_isbn li2 USING (rec_id)
                              WHERE li1.isbn != li2.isbn)
  SELECT isbn1, isbn2 FROM peer;
CREATE INDEX loc_isbn_peer_i1_idx ON loc_isbn_peer (isbn1);
CREATE INDEX loc_isbn_peer_i2_idx ON loc_isbn_peer (isbn2);

-- Now we need to identify a book ID for each ISBN cluster.
-- Our peer graph is not quite symmetrical; however, if we look up an isbn1, we find its entire peer group in isbn2
-- Approach: make a number for each ISBN, join to peers. Group by isbn2 and take the minimum book ID for each ISBN
DROP MATERIALIZED VIEW IF EXISTS loc_isbn_bookid;
CREATE MATERIALIZED VIEW loc_isbn_bookid
  AS SELECT isbn2 AS isbn, MIN(rec_id) AS book_id
     FROM loc_isbn JOIN loc_isbn_peer ON (isbn = isbn2)
     GROUP BY isbn2;
CREATE INDEX loc_isbn_bookid_idx ON loc_isbn_bookid (book_id);
CREATE INDEX loc_isbn_bookid_isbn_idx ON loc_isbn_bookid (isbn);

-- Extract authors
CREATE MATERIALIZED VIEW loc_author_name
  AS SELECT rec_id, regexp_replace(contents, '\W+$', '') AS name
  FROM loc_marc_field
  WHERE tag = '100' AND sf_code = 'a';
CREATE INDEX loc_author_name_rec_idx ON loc_author_name (rec_id);
CREATE INDEX loc_author_name_name_idx ON loc_author_name (name);

-- Set up Book ID tables
DROP TABLE IF EXISTS isbn_bookid;
CREATE TABLE isbn_bookid (
  isbn VARCHAR PRIMARY KEY,
  book_id INTEGER NOT NULL
);
CREATE INDEX isbn_bookid_idx ON isbn_bookid (book_id);
INSERT INTO isbn_bookid SELECT isbn, book_id FROM loc_isbn_bookid;
DROP SEQUENCe IF EXISTS synthetic_book_id;
CREATE SEQUENCE synthetic_book_id INCREMENT BY -1 START WITH -1;