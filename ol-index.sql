-- Create indexes and constraints
ALTER TABLE ol.author ADD PRIMARY KEY (author_id);
CREATE INDEX ol.author_key_idx ON ol.author (author_key);
ALTER TABLE ol.work ADD PRIMARY KEY (work_id);
CREATE INDEX ol.work_key_idx ON ol.work (work_key);
ALTER TABLE ol.edition ADD PRIMARY KEY (edition_id);
CREATE INDEX ol.edition_key_idx ON ol.edition (edition_key);

-- Set up work-author join table
CREATE TABLE ol.work_authors
AS SELECT work_id, author_id
   FROM ol.author
     JOIN (SELECT work_id, jsonb_array_elements((work_data->'authors')) #>> '{author,key}' AS author_key FROM ol.work) w
     USING (author_key);

CREATE INDEX ol.work_author_wk_idx ON ol.work_authors (work_id);
CREATE INDEX ol.work_author_au_idx ON ol.work_authors (author_id);
ALTER TABLE ol.work_authors ADD CONSTRAINT work_author_wk_fk FOREIGN KEY (work_id) REFERENCES ol.work;
ALTER TABLE ol.work_authors ADD CONSTRAINT work_author_au_fk FOREIGN KEY (author_id) REFERENCES ol.author;

CREATE TABLE ol.work_first_author
AS SELECT work_id, author_id
   FROM ol.author
     JOIN (SELECT work_id, work_data #>> '{authors,0,author,key}' AS author_key FROM ol.work) w
     USING (author_key);

CREATE INDEX ol.work_first_author_wk_idx ON ol.work_first_author (work_id);
CREATE INDEX ol.work_first_author_au_idx ON ol.work_first_author (author_id);
ALTER TABLE ol.work_first_author ADD CONSTRAINT work_first_author_wk_fk FOREIGN KEY (work_id) REFERENCES ol.work;
ALTER TABLE ol.work_first_author ADD CONSTRAINT work_first_author_au_fk FOREIGN KEY (author_id) REFERENCES ol.author;

-- Set up edition-author join table
CREATE TABLE ol.edition_author
AS SELECT edition_id, author_id
   FROM ol.author
     JOIN (SELECT edition_id, jsonb_array_elements((edition_data->'authors')) ->> 'key' AS author_key
           FROM ol.edition) e
     USING (author_key);

CREATE INDEX ol.edition_author_ed_idx ON ol.edition_author (edition_id);
CREATE INDEX ol.edition_author_au_idx ON ol.edition_author (author_id);
ALTER TABLE ol.edition_author ADD CONSTRAINT edition_author_wk_fk FOREIGN KEY (edition_id) REFERENCES ol.edition;
ALTER TABLE ol.edition_author ADD CONSTRAINT edition_author_au_fk FOREIGN KEY (author_id) REFERENCES ol.author;

CREATE TABLE ol.edition_first_author
AS SELECT edition_id, author_id
   FROM ol.author
     JOIN (SELECT edition_id, edition_data #>> '{authors,0,key}' AS author_key
           FROM ol.edition) e
     USING (author_key);

CREATE INDEX ol.edition_first_author_ed_idx ON ol.edition_first_author (edition_id);
CREATE INDEX ol.edition_first_author_au_idx ON ol.edition_first_author (author_id);
ALTER TABLE ol.edition_first_author ADD CONSTRAINT edition_first_author_wk_fk FOREIGN KEY (edition_id) REFERENCES ol.edition;
ALTER TABLE ol.edition_first_author ADD CONSTRAINT edition_first_author_au_fk FOREIGN KEY (author_id) REFERENCES ol.author;

-- Set up edition-work join table
CREATE TABLE ol.edition_work
AS SELECT edition_id, work_id
   FROM ol.work
     JOIN (SELECT edition_id, jsonb_array_elements((edition_data->'works')) ->> 'key' AS work_key FROM ol.edition) w
     USING (work_key);

CREATE INDEX ol.edition_work_ed_idx ON ol.edition_work (edition_id);
CREATE INDEX ol.edition_work_au_idx ON ol.edition_work (work_id);
ALTER TABLE ol.edition_work ADD CONSTRAINT edition_work_ed_fk FOREIGN KEY (edition_id) REFERENCES ol.edition;
ALTER TABLE ol.edition_work ADD CONSTRAINT edition_work_wk_fk FOREIGN KEY (work_id) REFERENCES ol.work;

-- Set up work and author summary info
CREATE MATERIALIZED VIEW ol.work_meta
  AS SELECT work_id, work_key, length(work_data::text) AS work_desc_length
    FROM ol.work;
CREATE INDEX ol.work_meta_work_idx ON ol.work_meta (work_id);
CREATE INDEX ol.work_meta_key_idx ON ol.work_meta (work_key);

CREATE MATERIALIZED VIEW ol.edition_meta
AS SELECT edition_id, edition_key, length(edition_data::text) AS edition_desc_length
   FROM ol.edition;
CREATE INDEX ol.edition_meta_edition_idx ON ol.edition_meta (edition_id);
CREATE INDEX ol.edition_meta_key_idx ON ol.edition_meta (edition_key);

-- Extract ISBNs (and ASINs)
CREATE MATERIALIZED VIEW ol_edition_isbn10
  AS SELECT edition_id, jsonb_array_elements_text(edition_data->'isbn_10') AS isbn
     FROM ol.edition;
CREATE MATERIALIZED VIEW ol_edition_isbn13
  AS SELECT edition_id, jsonb_array_elements_text(edition_data->'isbn_13') AS isbn
     FROM ol.edition;
CREATE MATERIALIZED VIEW ol_edition_asin
  AS SELECT edition_id, jsonb_array_elements_text(edition_data#>'{identifiers,amazon}') AS asin
     FROM ol.edition;

-- Integrate ISBN/ASIN identifiers
CREATE TABLE ol.edition_isbn (
  edition_id INTEGER NOT NULL,
  isbn VARCHAR NOT NULL
);

INSERT INTO ol.edition_isbn
  SELECT edition_id, isbn
  FROM (SELECT edition_id,
          regexp_replace(substring(upper(isbn) from '^\s*(?:(?:ISBN)?[:;z]?\s*)?([0-9 -]+[0-9X])'), '[- ]', '') AS isbn
        FROM ol_edition_isbn10) isbns
  WHERE isbn IS NOT NULL AND char_length(isbn) IN (10,13);
INSERT INTO ol.edition_isbn
  SELECT edition_id, isbn
  FROM (SELECT edition_id,
          regexp_replace(substring(upper(isbn) from '^\s*(?:(?:ISBN)?[:;z]?\s*)?([0-9 -]+[0-9X])'), '[- ]', '') AS isbn
        FROM ol_edition_isbn13) isbns
  WHERE isbn IS NOT NULL AND char_length(isbn) IN (10,13);
-- Don't bother with this, there are only 4K ASINs in the database
-- INSERT INTO ol.edition_isbn
--   SELECT edition_id, regexp_replace(upper(asin), '[- ]', '')
--   FROM ol_edition_asin
--   WHERE regexp_replace(upper(asin), '[- ]', '') ~ '^[A-Z0-9]{10}$';

CREATE INDEX ol.edition_isbn_ed_idx ON ol.edition_isbn (edition_id);
CREATE INDEX ol.edition_isbn_idx ON ol.edition_isbn (isbn);
ALTER TABLE ol.edition_isbn ADD CONSTRAINT edition_work_ed_fk FOREIGN KEY (edition_id) REFERENCES ol.edition;
ANALYZE ol.edition_isbn;

INSERT INTO isbn_id (isbn)
  SELECT DISTINCT isbn FROM ol.edition_isbn
  WHERE isbn NOT IN (SELECT isbn FROM isbn_id);
ANALYZE isbn_id;

CREATE TABLE ol.isbn_link (
  isbn_id INTEGER NOT NULL,
  edition_id INTEGER NOT NULL,
  work_id INTEGER NULL,
  book_code INTEGER NOT NULL
);
INSERT INTO ol.isbn_link
  SELECT isbn_id, edition_id, work_id,
       COALESCE(bc_of_work(work_id), bc_of_edition(edition_id))
    FROM isbn_id JOIN ol.edition_isbn USING (isbn) LEFT JOIN ol.edition_work USING (edition_id);
CREATE INDEX ol.isbn_link_ed_idx ON ol.isbn_link (edition_id);
CREATE INDEX ol.isbn_link_wk_idx ON ol.isbn_link (work_id);
CREATE INDEX ol.isbn_link_bc_idx ON ol.isbn_link (book_code);
CREATE INDEX ol.isbn_link_isbn_idx ON ol.isbn_link (isbn_id);
ALTER TABLE ol.isbn_link ADD CONSTRAINT ol_isbn_link_work_fk FOREIGN KEY (work_id) REFERENCES ol.work;
ALTER TABLE ol.isbn_link ADD CONSTRAINT ol_isbn_link_ed_fk FOREIGN KEY (edition_id) REFERENCES ol.edition;
ANALYZE ol.isbn_link;

-- Set up a general author names table, for all known names
CREATE TABLE ol.author_name (
  author_id INTEGER NOT NULL,
  author_name VARCHAR NOT NULL,
  name_source VARCHAR NOT NULL
);
INSERT INTO ol.author_name
SELECT author_id, author_data ->> 'name', 'name'
FROM ol.author WHERE author_data->>'name' IS NOT NULL;
INSERT INTO ol.author_name
SELECT author_id, author_data ->> 'personal_name', 'personal'
FROM ol.author WHERE author_data ? 'personal_name';
INSERT INTO ol.author_name
    SELECT author_id, jsonb_array_elements_text(author_data -> 'alternate_names'),
      'alternate'
FROM ol.author WHERE author_data ? 'alternate_names';
CREATE INDEX ol.author_name_idx ON ol.author_name (author_id);
CREATE INDEX ol.author_name_name_idx ON ol.author_name (author_name);
ANALYZE ol.author_name;

-- DROP MATERIALIZED VIEW IF EXISTS ol_book_first_author CASCADE;
-- CREATE MATERIALIZED VIEW ol_book_first_author
-- AS SELECT DISTINCT book_id, first_value(author_id) OVER (PARTITION BY book_id ORDER BY edition_desc_length) AS author_id
--    FROM ol_isbn_book_id
--      JOIN ol.edition_isbn USING (isbn)
--      JOIN ol.edition_first_author USING (edition_id)
--      JOIN ol.edition_meta USING (edition_id)
--    WHERE author_id IS NOT NULL;
-- CREATE INDEX book_first_author_book_idx ON ol_book_first_author (book_id);

CREATE MATERIALIZED VIEW ol.work_subject
AS SELECT work_id, jsonb_array_elements_text(work_data->'subjects') AS subject
  FROM ol.work;
CREATE INDEX ol.work_subject_work_idx ON ol.work_subject (work_id);
CREATE INDEX ol.work_subject_subj_idx ON ol.work_subject (subject);
