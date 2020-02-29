--- #dep gr-books
--- #dep gr-works
--- #dep gr-authors
--- #dep gr-book-genres
--- #step Add book PK
--- #allow invalid_table_definition
ALTER TABLE gr.raw_book ADD CONSTRAINT gr_raw_book_pk PRIMARY KEY (gr_book_rid);
--- #step Add work PK
--- #allow invalid_table_definition
ALTER TABLE gr.raw_work ADD CONSTRAINT gr_raw_work_pk PRIMARY KEY (gr_work_rid);
--- #step Add author PK
--- #allow invalid_table_definition
ALTER TABLE gr.raw_author ADD CONSTRAINT gr_raw_author_pk PRIMARY KEY (gr_author_rid);
--- #step Add book-genre PK
--- #allow invalid_table_definition
ALTER TABLE gr.raw_book_genres ADD CONSTRAINT gr_raw_book_genres_pk PRIMARY KEY (gr_book_genres_rid);

--- #step Extract work identifiers
CREATE TABLE IF NOT EXISTS gr.work_ids
  AS SELECT gr_work_rid, (gr_work_data->>'work_id')::int AS gr_work_id
     FROM gr.raw_work;

--- #step Index work identifiers
--- #allow duplicate_object
--- #allow invalid_table_definition
CREATE UNIQUE INDEX IF NOT EXISTS work_id_idx ON gr.work_ids (gr_work_id);
ALTER TABLE gr.work_ids ADD CONSTRAINT gr_work_id_pk PRIMARY KEY (gr_work_rid);
ALTER TABLE gr.work_ids ADD CONSTRAINT gr_work_id_fk FOREIGN KEY (gr_work_rid) REFERENCES gr.raw_work (gr_work_rid);
ANALYZE gr.work_ids;

--- #step Extract book identifiers
CREATE TABLE IF NOT EXISTS gr.book_ids
  AS SELECT gr_book_rid,
            NULLIF(work_id, '')::int AS gr_work_id,
            book_id AS gr_book_id,
            NULLIF(trim(both from asin), '') AS gr_asin,
            NULLIF(trim(both from isbn), '') AS gr_isbn,
            NULLIF(trim(both from isbn13), '') AS gr_isbn13
     FROM gr.raw_book,
          jsonb_to_record(gr_book_data) AS x(work_id VARCHAR, book_id INTEGER, asin VARCHAR, isbn VARCHAR, isbn13 VARCHAR);

--- #step Index book identifiers
--- #allow invalid_table_definition
-- If we have completed this step, the PK add will fail with invalid table definition
ALTER TABLE gr.book_ids ADD CONSTRAINT gr_book_id_pk PRIMARY KEY (gr_book_rid);
CREATE UNIQUE INDEX book_id_idx ON gr.book_ids (gr_book_id);
CREATE INDEX book_work_idx ON gr.book_ids (gr_work_id);
CREATE INDEX book_asin_idx ON gr.book_ids (gr_asin);
CREATE INDEX book_isbn_idx ON gr.book_ids (gr_isbn);
CREATE INDEX book_isbn13_idx ON gr.book_ids (gr_isbn13);
ALTER TABLE gr.book_ids ADD CONSTRAINT gr_book_id_fk FOREIGN KEY (gr_book_rid) REFERENCES gr.raw_book (gr_book_rid);
ALTER TABLE gr.book_ids ADD CONSTRAINT gr_book_id_work_fk FOREIGN KEY (gr_work_id) REFERENCES gr.work_ids (gr_work_id);
ANALYZE gr.book_ids;

--- #step Update ISBN ID records with new ISBNs from GoodReads
INSERT INTO isbn_id (isbn)
SELECT DISTINCT gr_isbn FROM gr.book_ids
WHERE gr_isbn IS NOT NULL AND gr_isbn NOT IN (SELECT isbn FROM isbn_id);
INSERT INTO isbn_id (isbn)
SELECT DISTINCT gr_isbn13 FROM gr.book_ids
WHERE gr_isbn13 IS NOT NULL AND gr_isbn13 NOT IN (SELECT isbn FROM isbn_id);

--- #step Update ISBN ID records with ASINs from GoodReads
INSERT INTO isbn_id (isbn)
SELECT DISTINCT gr_asin FROM gr.book_ids
WHERE gr_asin IS NOT NULL AND gr_asin NOT IN (SELECT isbn FROM isbn_id);

--- #step Map ISBNs to book IDs
CREATE TABLE IF NOT EXISTS gr.book_isbn
  AS SELECT gr_book_id, isbn_id, COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id)) AS book_code
  FROM gr.book_ids, isbn_id
  WHERE isbn = gr_isbn OR isbn = gr_isbn13;

--- #step Index GoodReads ISBNs
--- #allow duplicate_object
--- #allow duplicate_table
-- this will fail promptly with duplicate object if the index already exists
CREATE INDEX book_isbn_book_idx ON gr.book_isbn (gr_book_id);
CREATE INDEX book_isbn_isbn_idx ON gr.book_isbn (isbn_id);
CREATE INDEX book_isbn_code_idx ON gr.book_isbn (book_code);
ALTER TABLE gr.book_isbn ADD CONSTRAINT gr_book_isbn_book_fk FOREIGN KEY (gr_book_id) REFERENCES gr.book_ids (gr_book_id);
ALTER TABLE gr.book_isbn ADD CONSTRAINT gr_book_isbn_isbn_fk FOREIGN KEY (isbn_id) REFERENCES isbn_id (isbn_id);
ANALYZE gr.book_isbn;

--- #step Index GoodReads book genres
--- #allow duplicate_table
CREATE TABLE IF NOT EXISTS gr.book_genres
  AS SELECT gr_book_rid, gr_book_id, key AS genre, value AS score
       FROM gr.raw_book_genres, jsonb_each_text(gr_book_genres_data->'genres'),
            gr.book_ids
     WHERE gr_book_id = (gr_book_genres_data->>'book_id')::int;
CREATE INDEX bg_book_rid ON gr.book_genres (gr_book_rid);
CREATE INDEX bg_book_id ON gr.book_genres (gr_book_id);
