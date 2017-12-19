-- Create indexes and constraints
ALTER TABLE ol_author ADD PRIMARY KEY (author_id);
ALTER TABLE ol_author ADD CONSTRAINT author_key_uq UNIQUE (author_key);
ALTER TABLE ol_work ADD PRIMARY KEY (work_id);
ALTER TABLE ol_work ADD CONSTRAINT work_key_uq UNIQUE (work_key);
ALTER TABLE ol_edition ADD PRIMARY KEY (edition_id);
ALTER TABLE ol_edition ADD CONSTRAINT edition_key_uq UNIQUE (edition_key);

-- Set up work-author join table
DROP TABLE IF EXISTS work_authors CASCADE;
CREATE TABLE work_authors
AS SELECT work_id, author_id
   FROM ol_author
     JOIN (SELECT work_id, jsonb_array_elements((work_data->'authors')) #>> '{author,key}' AS author_key FROM ol_work) w
     USING (author_key);

CREATE INDEX work_author_wk_idx ON work_authors (work_id);
CREATE INDEX work_author_au_idx ON work_authors (author_id);
ALTER TABLE work_authors ADD CONSTRAINT work_author_wk_fk FOREIGN KEY (work_id) REFERENCES ol_work;
ALTER TABLE work_authors ADD CONSTRAINT work_author_au_fk FOREIGN KEY (author_id) REFERENCES ol_author;

DROP TABLE IF EXISTS work_first_author CASCADE;
CREATE TABLE work_first_author
AS SELECT work_id, author_id
   FROM ol_author
     JOIN (SELECT work_id, work_data #>> '{authors,0,author,key}' AS author_key FROM ol_work) w
     USING (author_key);

CREATE INDEX work_first_author_wk_idx ON work_first_author (work_id);
CREATE INDEX work_first_author_au_idx ON work_first_author (author_id);
ALTER TABLE work_first_author ADD CONSTRAINT work_first_author_wk_fk FOREIGN KEY (work_id) REFERENCES ol_work;
ALTER TABLE work_first_author ADD CONSTRAINT work_first_author_au_fk FOREIGN KEY (author_id) REFERENCES ol_author;

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
CREATE INDEX work_meta_work_idx ON ol_work_meta (work_id);
CREATE INDEX work_meta_key_idx ON ol_work_meta (work_key);

DROP MATERIALIZED VIEW IF EXISTS ol_edition_meta;
CREATE MATERIALIZED VIEW ol_edition_meta
AS SELECT edition_id, edition_key, length(edition_data::text) AS edition_desc_length
   FROM ol_edition;
CREATE INDEX edition_meta_edition_idx ON ol_edition_meta (edition_id);
CREATE INDEX edition_meta_key_idx ON ol_edition_meta (edition_key);

-- Extract ISBNs
DROP TABLE IF EXISTS ol_edition_isbn;
CREATE TABLE ol_edition_isbn
AS SELECT edition_id, jsonb_array_elements_text(edition_data->'isbn_10') AS isbn
   FROM ol_edition
   UNION
   SELECT edition_id, jsonb_array_elements_text(edition_data->'isbn_13') AS isbn
   FROM ol_edition;

CREATE INDEX edition_isbn_ed_idx ON ol_edition_isbn (edition_id);
CREATE INDEX edition_isbn_idx ON ol_edition_isbn (isbn);
ALTER TABLE ol_edition_isbn ADD CONSTRAINT edition_work_ed_fk FOREIGN KEY (edition_id) REFERENCES ol_edition;

-- Make ID mapping
-- ID mod 3 is type: 0 synthetic, 1 work id, 2 edition id
DROP TABLE IF EXISTS isbn_info;
CREATE TABLE isbn_info
  AS SELECT isbn, edition_id, work_id, COALESCE(work_id * 3 - 2, edition_id * 3 - 1) AS book_id
  FROM ol_edition_isbn
  LEFT OUTER JOIN (SELECT edition_id, MIN(work_id) AS work_id
                   FROM ol_edition_work
                   GROUP BY edition_id) AS ew USING (edition_id);

CREATE INDEX isbn_info_isbn_idx ON isbn_info (isbn);
CREATE INDEX isbn_info_edition_idx ON isbn_info (edition_id);
CREATE INDEX isbn_info_work_idx ON isbn_info (work_id);
CREATE INDEX isbn_info_book_idx ON isbn_info (book_id);

CREATE SEQUENCE synthetic_book_id_seq;

DROP MATERIALIZED VIEW IF EXISTS isbn_book_id;
CREATE MATERIALIZED VIEW isbn_book_id
  AS SELECT isbn, MIN(book_id) AS book_id FROM isbn_info GROUP BY isbn;

CREATE INDEX isbn_book_id_isbn_idx ON isbn_book_id (isbn);
CREATE INDEX isbn_book_id_idx ON isbn_book_id (book_id);

DROP MATERIALIZED VIEW IF EXISTS ol_book_first_author;
CREATE MATERIALIZED VIEW ol_book_first_author
AS SELECT book_id, first_value(author_id) OVER (PARTITION BY book_id ORDER BY edition_desc_length) AS author_id
   FROM isbn_book_id
     JOIN ol_edition_isbn USING (isbn)
     JOIN ol_edition_first_author USING (edition_id)
     JOIN ol_edition_meta USING (edition_id)
   WHERE author_id IS NOT NULL;
CREATE INDEX book_first_author_book_idx ON ol_book_first_author (book_id);

ANALYZE;
