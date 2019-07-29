--- #step Index OL author table
--- #allow invalid_table_definition
CREATE INDEX IF NOT EXISTS author_key_idx ON ol.author (author_key);
ALTER TABLE ol.author ADD PRIMARY KEY (author_id);
--- #step Index OL work table
--- #allow invalid_table_definition
CREATE INDEX IF NOT EXISTS work_key_idx ON ol.work (work_key);
ALTER TABLE ol.work ADD PRIMARY KEY (work_id);
--- #step Index OL edition table
--- #allow invalid_table_definition
CREATE INDEX IF NOT EXISTS  edition_key_idx ON ol.edition (edition_key);
ALTER TABLE ol.edition ADD PRIMARY KEY (edition_id);

--- #step Set up work-author join table
CREATE TABLE IF NOT EXISTS ol.work_authors
AS SELECT work_id, author_id
   FROM ol.author
     JOIN (SELECT work_id, jsonb_array_elements((work_data->'authors')) #>> '{author,key}' AS author_key FROM ol.work) w
     USING (author_key);

--- #step Index work-author join table
CREATE INDEX work_author_wk_idx ON ol.work_authors (work_id);
CREATE INDEX work_author_au_idx ON ol.work_authors (author_id);
--- #step Author work FK
--- #allow duplicate_object
ALTER TABLE ol.work_authors ADD CONSTRAINT work_author_wk_fk FOREIGN KEY (work_id) REFERENCES ol.work;
--- #step Author-work author FK
--- #allow duplicate_object
ALTER TABLE ol.work_authors ADD CONSTRAINT work_author_au_fk FOREIGN KEY (author_id) REFERENCES ol.author;

--- #step Set up work first author table
CREATE TABLE IF NOT EXISTS ol.work_first_author
AS SELECT work_id, author_id
   FROM ol.author
     JOIN (SELECT work_id, work_data #>> '{authors,0,author,key}' AS author_key FROM ol.work) w
     USING (author_key);

--- #step Index work first author table
CREATE INDEX IF NOT EXISTS work_first_author_wk_idx ON ol.work_first_author (work_id);
CREATE INDEX IF NOT EXISTS work_first_author_au_idx ON ol.work_first_author (author_id);
--- #step First author work FK
--- #allow duplicate_object
ALTER TABLE ol.work_first_author ADD CONSTRAINT work_first_author_wk_fk FOREIGN KEY (work_id) REFERENCES ol.work;
--- #step First author author FK
--- #allow duplicate_object
ALTER TABLE ol.work_first_author ADD CONSTRAINT work_first_author_au_fk FOREIGN KEY (author_id) REFERENCES ol.author;

--- #step Set up edition-author join table
CREATE TABLE IF NOT EXISTS ol.edition_author
AS SELECT edition_id, author_id
   FROM ol.author
     JOIN (SELECT edition_id, jsonb_array_elements((edition_data->'authors')) ->> 'key' AS author_key
           FROM ol.edition) e
     USING (author_key);

--- #step index edition-author join table
CREATE INDEX IF NOT EXISTS edition_author_ed_idx ON ol.edition_author (edition_id);
CREATE INDEX IF NOT EXISTS edition_author_au_idx ON ol.edition_author (author_id);
--- #allow duplicate_object
ALTER TABLE ol.edition_author ADD CONSTRAINT edition_author_wk_fk FOREIGN KEY (edition_id) REFERENCES ol.edition;
--- #allow duplicate_object
ALTER TABLE ol.edition_author ADD CONSTRAINT edition_author_au_fk FOREIGN KEY (author_id) REFERENCES ol.author;

--- #step Create edition first-author table
CREATE TABLE ol.edition_first_author
AS SELECT edition_id, author_id
   FROM ol.author
     JOIN (SELECT edition_id, edition_data #>> '{authors,0,key}' AS author_key
           FROM ol.edition) e
     USING (author_key);

--- #step Index edition first-author table
CREATE INDEX IF NOT EXISTS edition_first_author_ed_idx ON ol.edition_first_author (edition_id);
CREATE INDEX IF NOT EXISTS edition_first_author_au_idx ON ol.edition_first_author (author_id);
--- #step Edition first author edition FK
--- #allow duplicate_object
ALTER TABLE ol.edition_first_author ADD CONSTRAINT edition_first_author_wk_fk FOREIGN KEY (edition_id) REFERENCES ol.edition;
--- #step Edition first author author FK
--- #allow duplicate_object
ALTER TABLE ol.edition_first_author ADD CONSTRAINT edition_first_author_au_fk FOREIGN KEY (author_id) REFERENCES ol.author;

--- #step Set up edition-work join table
CREATE TABLE IF NOT EXISTS ol.edition_work
AS SELECT edition_id, work_id
   FROM ol.work
     JOIN (SELECT edition_id, jsonb_array_elements((edition_data->'works')) ->> 'key' AS work_key FROM ol.edition) w
     USING (work_key);

--- #step Index edition-work join table
CREATE INDEX IF NOT EXISTS edition_work_ed_idx ON ol.edition_work (edition_id);
CREATE INDEX IF NOT EXISTS edition_work_au_idx ON ol.edition_work (work_id);
--- #step Edition-work edition FK
--- #allow duplicate_object
ALTER TABLE ol.edition_work ADD CONSTRAINT edition_work_ed_fk FOREIGN KEY (edition_id) REFERENCES ol.edition;
--- #step Edition-work work FK
--- #allow duplicate_object
ALTER TABLE ol.edition_work ADD CONSTRAINT edition_work_wk_fk FOREIGN KEY (work_id) REFERENCES ol.work;

--- #step Extract ISBNs and ASINs
CREATE MATERIALIZED VIEW IF NOT EXISTS ol.edition_isbn10
  AS SELECT edition_id, jsonb_array_elements_text(edition_data->'isbn_10') AS isbn
     FROM ol.edition;
CREATE MATERIALIZED VIEW IF NOT EXISTS ol.edition_isbn13
  AS SELECT edition_id, jsonb_array_elements_text(edition_data->'isbn_13') AS isbn
     FROM ol.edition;
CREATE MATERIALIZED VIEW IF NOT EXISTS ol.edition_asin
  AS SELECT edition_id, jsonb_array_elements_text(edition_data#>'{identifiers,amazon}') AS asin
     FROM ol.edition;

--- #step Integrate ISBN/ASIN identifiers
DROP TABLE IF EXISTS ol.edition_isbn CASCADE;
CREATE TABLE ol.edition_isbn (
  edition_id INTEGER NOT NULL,
  isbn VARCHAR NOT NULL
);

WITH
  ol_edition_isbn10
    AS (SELECT edition_id, jsonb_array_elements_text(edition_data->'isbn_10') AS isbn
        FROM ol.edition)
INSERT INTO ol.edition_isbn
  SELECT edition_id, isbn
  FROM (SELECT edition_id, extract_isbn(isbn) AS isbn
        FROM ol_edition_isbn10) isbns
  WHERE isbn IS NOT NULL AND char_length(isbn) IN (10,13);

WITH
  ol_edition_isbn13
    AS (SELECT edition_id, jsonb_array_elements_text(edition_data->'isbn_13') AS isbn
        FROM ol.edition)
INSERT INTO ol.edition_isbn
  SELECT edition_id, isbn
  FROM (SELECT edition_id, extract_isbn(isbn) AS isbn
        FROM ol_edition_isbn13) isbns
  WHERE isbn IS NOT NULL AND char_length(isbn) IN (10,13);

WITH
  ol_edition_asin AS
    (SELECT edition_id, jsonb_array_elements_text(edition_data#>'{identifiers,amazon}') AS asin
     FROM ol.edition)
INSERT INTO ol.edition_isbn
  SELECT edition_id, regexp_replace(upper(asin), '[- ]', '')
  FROM ol_edition_asin
  WHERE regexp_replace(upper(asin), '[- ]', '') ~ '^[A-Z0-9]{10}$';

CREATE INDEX edition_isbn_ed_idx ON ol.edition_isbn (edition_id);
CREATE INDEX edition_isbn_idx ON ol.edition_isbn (isbn);
ALTER TABLE ol.edition_isbn ADD CONSTRAINT edition_work_ed_fk FOREIGN KEY (edition_id) REFERENCES ol.edition;
ANALYZE ol.edition_isbn;

--- #step Add OL ISBNs to global table
INSERT INTO isbn_id (isbn)
  SELECT DISTINCT isbn FROM ol.edition_isbn
  WHERE isbn NOT IN (SELECT DISTINCT isbn FROM isbn_id);
ANALYZE isbn_id;

--- #step Link OL ISBNs
DROP TABLE IF EXISTS ol.isbn_link CASCADE;
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
CREATE INDEX isbn_link_ed_idx ON ol.isbn_link (edition_id);
CREATE INDEX isbn_link_wk_idx ON ol.isbn_link (work_id);
CREATE INDEX isbn_link_bc_idx ON ol.isbn_link (book_code);
CREATE INDEX isbn_link_isbn_idx ON ol.isbn_link (isbn_id);
ALTER TABLE ol.isbn_link ADD CONSTRAINT ol_isbn_link_work_fk FOREIGN KEY (work_id) REFERENCES ol.work;
ALTER TABLE ol.isbn_link ADD CONSTRAINT ol_isbn_link_ed_fk FOREIGN KEY (edition_id) REFERENCES ol.edition;
ANALYZE ol.isbn_link;

--- #step Tabulate author names
DROP TABLE IF EXISTS ol.author_name CASCADE;
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
CREATE INDEX author_name_idx ON ol.author_name (author_id);
CREATE INDEX author_name_name_idx ON ol.author_name (author_name);
ANALYZE ol.author_name;

--- #step Catalog work subjects
CREATE MATERIALIZED VIEW IF NOT EXISTS ol.work_subject
AS SELECT work_id, jsonb_array_elements_text(work_data->'subjects') AS subject
  FROM ol.work;
CREATE INDEX IF NOT EXISTS work_subject_work_idx ON ol.work_subject (work_id);
CREATE INDEX IF NOT EXISTS work_subject_subj_idx ON ol.work_subject (subject);
