-- Create indexes and constraints
ALTER TABLE authors ADD PRIMARY KEY (author_id);
ALTER TABLE authors ADD CONSTRAINT author_key_uq UNIQUE (author_key);
ALTER TABLE works ADD PRIMARY KEY (work_id);
ALTER TABLE works ADD CONSTRAINT work_key_uq UNIQUE (work_key);
ALTER TABLE editions ADD PRIMARY KEY (edition_id);
ALTER TABLE editions ADD CONSTRAINT edition_key_uq UNIQUE (edition_key);

-- Set up work-author join table
DROP TABLE IF EXISTS work_authors CASCADE;
CREATE TABLE work_authors
AS SELECT work_id, author_id
   FROM authors
     JOIN (SELECT work_id, jsonb_array_elements((work_data->'authors')) #>> '{author,key}' AS author_key FROM works) w
     USING (author_key);

CREATE INDEX work_author_wk_idx ON work_authors (work_id);
CREATE INDEX work_author_au_idx ON work_authors (author_id);
ALTER TABLE work_authors ADD CONSTRAINT work_author_wk_fk FOREIGN KEY (work_id) REFERENCES works;
ALTER TABLE work_authors ADD CONSTRAINT work_author_au_fk FOREIGN KEY (author_id) REFERENCES authors;

DROP TABLE IF EXISTS work_first_author CASCADE;
CREATE TABLE work_first_author
AS SELECT work_id, author_id
   FROM authors
     JOIN (SELECT work_id, work_data #>> '{authors,0,author,key}' AS author_key FROM works) w
     USING (author_key);

CREATE INDEX work_first_author_wk_idx ON work_first_author (work_id);
CREATE INDEX work_first_author_au_idx ON work_first_author (author_id);
ALTER TABLE work_first_author ADD CONSTRAINT work_first_author_wk_fk FOREIGN KEY (work_id) REFERENCES works;
ALTER TABLE work_first_author ADD CONSTRAINT work_first_author_au_fk FOREIGN KEY (author_id) REFERENCES authors;

-- Set up edition-author join table
DROP TABLE IF EXISTS edition_authors;
CREATE TABLE edition_authors
AS SELECT edition_id, author_id
   FROM authors
     JOIN (SELECT edition_id, jsonb_array_elements((edition_data->'authors')) ->> 'key' AS author_key
           FROM editions) e
     USING (author_key);

CREATE INDEX edition_author_ed_idx ON edition_authors (edition_id);
CREATE INDEX edition_author_au_idx ON edition_authors (author_id);
ALTER TABLE edition_authors ADD CONSTRAINT edition_author_wk_fk FOREIGN KEY (edition_id) REFERENCES editions;
ALTER TABLE edition_authors ADD CONSTRAINT edition_author_au_fk FOREIGN KEY (author_id) REFERENCES authors;

DROP TABLE IF EXISTS edition_first_author;
CREATE TABLE edition_first_author
AS SELECT edition_id, author_id
   FROM authors
     JOIN (SELECT edition_id, edition_data #>> '{authors,0,key}' AS author_key
           FROM editions) e
     USING (author_key);

CREATE INDEX edition_first_author_ed_idx ON edition_first_author (edition_id);
CREATE INDEX edition_first_author_au_idx ON edition_first_author (author_id);
ALTER TABLE edition_first_author ADD CONSTRAINT edition_first_author_wk_fk FOREIGN KEY (edition_id) REFERENCES editions;
ALTER TABLE edition_first_author ADD CONSTRAINT edition_first_author_au_fk FOREIGN KEY (author_id) REFERENCES authors;

-- Set up edition-work join table
DROP TABLE IF EXISTS edition_works;
CREATE TABLE edition_works
AS SELECT edition_id, work_id
   FROM works
     JOIN (SELECT edition_id, jsonb_array_elements((edition_data->'works')) ->> 'key' AS work_key FROM editions) w
     USING (work_key);

CREATE INDEX edition_work_ed_idx ON edition_works (edition_id);
CREATE INDEX edition_work_au_idx ON edition_works (work_id);
ALTER TABLE edition_works ADD CONSTRAINT edition_work_ed_fk FOREIGN KEY (edition_id) REFERENCES editions;
ALTER TABLE edition_works ADD CONSTRAINT edition_work_wk_fk FOREIGN KEY (work_id) REFERENCES works;

-- Extract ISBNs
DROP TABLE IF EXISTS edition_isbn;
CREATE TABLE edition_isbn
AS SELECT edition_id, jsonb_array_elements_text(edition_data->'isbn_10') AS isbn
   FROM editions
   UNION
   SELECT edition_id, jsonb_array_elements_text(edition_data->'isbn_13') AS isbn
   FROM editions;

CREATE INDEX edition_isbn_ed_idx ON edition_isbn (edition_id);
CREATE INDEX edition_isbn_idx ON edition_isbn (isbn);
ALTER TABLE edition_isbn ADD CONSTRAINT edition_work_ed_fk FOREIGN KEY (edition_id) REFERENCES editions;

-- Make ID mapping
-- ID mod 3 is type: 0 synthetic, 1 work id, 2 edition id
DROP TABLE IF EXISTS isbn_info;
CREATE TABLE isbn_info
  AS SELECT isbn, edition_id, work_id, COALESCE(work_id * 3 - 2, edition_id * 3 - 1) AS book_id
  FROM edition_isbn
  LEFT OUTER JOIN (SELECT edition_id, MIN(work_id) AS work_id
                   FROM edition_works
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

ANALYZE;