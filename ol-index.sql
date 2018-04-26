-- Create indexes and constraints
ALTER TABLE ol_author ADD PRIMARY KEY (author_id);
ALTER TABLE ol_author ADD CONSTRAINT author_key_uq UNIQUE (author_key);
ALTER TABLE ol_work ADD PRIMARY KEY (work_id);
ALTER TABLE ol_work ADD CONSTRAINT work_key_uq UNIQUE (work_key);
ALTER TABLE ol_edition ADD PRIMARY KEY (edition_id);
ALTER TABLE ol_edition ADD CONSTRAINT edition_key_uq UNIQUE (edition_key);

-- Set up work-author join table
DROP TABLE IF EXISTS ol_work_authors CASCADE;
CREATE TABLE ol_work_authors
AS SELECT work_id, author_id
   FROM ol_author
     JOIN (SELECT work_id, jsonb_array_elements((work_data->'authors')) #>> '{author,key}' AS author_key FROM ol_work) w
     USING (author_key);

CREATE INDEX work_author_wk_idx ON ol_work_authors (work_id);
CREATE INDEX work_author_au_idx ON ol_work_authors (author_id);
ALTER TABLE ol_work_authors ADD CONSTRAINT work_author_wk_fk FOREIGN KEY (work_id) REFERENCES ol_work;
ALTER TABLE ol_work_authors ADD CONSTRAINT work_author_au_fk FOREIGN KEY (author_id) REFERENCES ol_author;

DROP TABLE IF EXISTS ol_work_first_author CASCADE;
CREATE TABLE ol_work_first_author
AS SELECT work_id, author_id
   FROM ol_author
     JOIN (SELECT work_id, work_data #>> '{authors,0,author,key}' AS author_key FROM ol_work) w
     USING (author_key);

CREATE INDEX work_first_author_wk_idx ON ol_work_first_author (work_id);
CREATE INDEX work_first_author_au_idx ON ol_work_first_author (author_id);
ALTER TABLE ol_work_first_author ADD CONSTRAINT work_first_author_wk_fk FOREIGN KEY (work_id) REFERENCES ol_work;
ALTER TABLE ol_work_first_author ADD CONSTRAINT work_first_author_au_fk FOREIGN KEY (author_id) REFERENCES ol_author;

-- Set up edition-author join table
DROP TABLE IF EXISTS ol_edition_author;
CREATE TABLE ol_edition_author
AS SELECT edition_id, author_id
   FROM ol_author
     JOIN (SELECT edition_id, jsonb_array_elements((edition_data->'authors')) ->> 'key' AS author_key
           FROM ol_edition) e
     USING (author_key);

CREATE INDEX edition_author_ed_idx ON ol_edition_author (edition_id);
CREATE INDEX edition_author_au_idx ON ol_edition_author (author_id);
ALTER TABLE ol_edition_author ADD CONSTRAINT edition_author_wk_fk FOREIGN KEY (edition_id) REFERENCES ol_edition;
ALTER TABLE ol_edition_author ADD CONSTRAINT edition_author_au_fk FOREIGN KEY (author_id) REFERENCES ol_author;

DROP TABLE IF EXISTS ol_edition_first_author;
CREATE TABLE ol_edition_first_author
AS SELECT edition_id, author_id
   FROM ol_author
     JOIN (SELECT edition_id, edition_data #>> '{authors,0,key}' AS author_key
           FROM ol_edition) e
     USING (author_key);

CREATE INDEX edition_first_author_ed_idx ON ol_edition_first_author (edition_id);
CREATE INDEX edition_first_author_au_idx ON ol_edition_first_author (author_id);
ALTER TABLE ol_edition_first_author ADD CONSTRAINT edition_first_author_wk_fk FOREIGN KEY (edition_id) REFERENCES ol_edition;
ALTER TABLE ol_edition_first_author ADD CONSTRAINT edition_first_author_au_fk FOREIGN KEY (author_id) REFERENCES ol_author;

-- Set up edition-work join table
DROP TABLE IF EXISTS ol_edition_work;
CREATE TABLE ol_edition_work
AS SELECT edition_id, work_id
   FROM ol_work
     JOIN (SELECT edition_id, jsonb_array_elements((edition_data->'works')) ->> 'key' AS work_key FROM ol_edition) w
     USING (work_key);

CREATE INDEX edition_work_ed_idx ON ol_edition_work (edition_id);
CREATE INDEX edition_work_au_idx ON ol_edition_work (work_id);
ALTER TABLE ol_edition_work ADD CONSTRAINT edition_work_ed_fk FOREIGN KEY (edition_id) REFERENCES ol_edition;
ALTER TABLE ol_edition_work ADD CONSTRAINT edition_work_wk_fk FOREIGN KEY (work_id) REFERENCES ol_work;

-- Set up work and author summary info
DROP MATERIALIZED VIEW IF EXISTS ol_work_meta;
CREATE MATERIALIZED VIEW ol_work_meta
  AS SELECT work_id, work_key, length(work_data::text) AS work_desc_length
    FROM ol_work;
CREATE INDEX ol_work_meta_work_idx ON ol_work_meta (work_id);
CREATE INDEX ol_work_meta_key_idx ON ol_work_meta (work_key);

DROP MATERIALIZED VIEW IF EXISTS ol_edition_meta;
CREATE MATERIALIZED VIEW ol_edition_meta
AS SELECT edition_id, edition_key, length(edition_data::text) AS edition_desc_length
   FROM ol_edition;
CREATE INDEX ol_edition_meta_edition_idx ON ol_edition_meta (edition_id);
CREATE INDEX ol_edition_meta_key_idx ON ol_edition_meta (edition_key);

-- Extract ISBNs
DROP TABLE IF EXISTS ol_edition_isbn;
CREATE TABLE ol_edition_isbn
AS SELECT edition_id, upper(jsonb_array_elements_text(edition_data->'isbn_10')) AS isbn
   FROM ol_edition
   UNION
   SELECT edition_id, upper(jsonb_array_elements_text(edition_data->'isbn_13')) AS isbn
   FROM ol_edition;

CREATE INDEX edition_isbn_ed_idx ON ol_edition_isbn (edition_id);
CREATE INDEX edition_isbn_idx ON ol_edition_isbn (isbn);
ALTER TABLE ol_edition_isbn ADD CONSTRAINT edition_work_ed_fk FOREIGN KEY (edition_id) REFERENCES ol_edition;

DROP TABLE IF EXISTS ol_isbn_id CASCADE;
CREATE TABLE ol_isbn_id (
  isbn_id SERIAL PRIMARY KEY,
  isbn VARCHAR NOT NULL
);
INSERT INTO ol_isbn_id (isbn) 
SELECT DISTINCT regexp_replace(isbn, '[- ]', '')
FROM ol_edition_isbn
WHERE char_length(regexp_replace(isbn, '[- ]', '')) IN (10, 13);
CREATE INDEX ol_isbn_id_isbn_uq ON ol_isbn_id USING HASH (isbn);
ANALYZE ol_isbn_id;

DROP TABLE IF EXISTS ol_isbn_link;
CREATE TABLE ol_isbn_link (
  isbn_id INTEGER NOT NULL,
  edition_id INTEGER NOT NULL,
  work_id INTEGER NULL,
  book_code INTEGER NOT NULL
);
INSERT INTO ol_isbn_link
  SELECT isbn_id, edition_id, work_id,
       COALESCE(100000000 + work_id, 200000000 + edition_id)
    FROM ol_isbn_id JOIN ol_edition_isbn USING (isbn) LEFT JOIN ol_edition_work USING (edition_id);
CREATE INDEX ol_isbn_link_ed_idx ON ol_isbn_link (edition_id);
CREATE INDEX ol_isbn_link_wk_idx ON ol_isbn_link (work_id);
CREATE INDEX ol_isbn_link_bc_idx ON ol_isbn_link (book_code);
CREATE INDEX ol_isbn_link_isbn_idx ON ol_isbn_link (isbn_id);
ALTER TABLE ol_isbn_link ADD CONSTRAINT ol_isbn_link_work_fk FOREIGN KEY (work_id) REFERENCES ol_work;
ALTER TABLE ol_isbn_link ADD CONSTRAINT ol_isbn_link_ed_fk FOREIGN KEY (edition_id) REFERENCES ol_edition;

-- DROP MATERIALIZED VIEW IF EXISTS ol_book_first_author CASCADE;
-- CREATE MATERIALIZED VIEW ol_book_first_author
-- AS SELECT DISTINCT book_id, first_value(author_id) OVER (PARTITION BY book_id ORDER BY edition_desc_length) AS author_id
--    FROM ol_isbn_book_id
--      JOIN ol_edition_isbn USING (isbn)
--      JOIN ol_edition_first_author USING (edition_id)
--      JOIN ol_edition_meta USING (edition_id)
--    WHERE author_id IS NOT NULL;
-- CREATE INDEX book_first_author_book_idx ON ol_book_first_author (book_id);

DROP MATERIALIZED VIEW IF EXISTS ol_work_subject CASCADE;
CREATE MATERIALIZED VIEW ol_work_subject
AS SELECT work_id, jsonb_array_elements_text(work_data->'subjects') AS subject
  FROM ol_work;
CREATE INDEX ol_work_subject_work_idx ON ol_work_subject (work_id);
CREATE INDEX ol_work_subject_subj_idx ON ol_work_subject (subject);

ANALYZE;
