-- Index GoodReads book data
ALTER TABLE gr_raw_book ADD CONSTRAINT gr_raw_book_pk PRIMARY KEY (gr_book_rid);
ALTER TABLE gr_raw_work ADD CONSTRAINT gr_raw_work_pk PRIMARY KEY (gr_work_rid);
ALTER TABLE gr_raw_author ADD CONSTRAINT gr_raw_author_pk PRIMARY KEY (gr_author_rid);
ALTER TABLE gr_raw_interaction ADD CONSTRAINT gr_raw_interaction_pk PRIMARY KEY (gr_int_rid);

-- Extract work identifiers from GoodReads work records
CREATE TABLE gr_work_ids
  AS SELECT gr_work_rid, (gr_work_data->>'work_id')::int AS gr_work_id
     FROM gr_raw_work;
ALTER TABLE gr_work_ids ADD CONSTRAINT gr_work_id_pk PRIMARY KEY (gr_work_rid);
CREATE UNIQUE INDEX gr_work_id_idx ON gr_work_ids (gr_work_id);
ALTER TABLE gr_work_ids ADD CONSTRAINT gr_work_id_fk FOREIGN KEY (gr_work_rid) REFERENCES gr_raw_work (gr_work_rid);
ANALYZE gr_work_ids;

-- Extract book identifiers from GoodReads book records
CREATE TABLE gr_book_ids
  AS SELECT gr_book_rid,
            NULLIF(work_id, '')::int AS gr_work_id,
            book_id AS gr_book_id,
            NULLIF(trim(both from asin), '') AS gr_asin,
            NULLIF(trim(both from isbn), '') AS gr_isbn,
            NULLIF(trim(both from isbn13), '') AS gr_isbn13
     FROM gr_raw_book,
          jsonb_to_record(gr_book_data) AS x(work_id VARCHAR, book_id INTEGER, asin VARCHAR, isbn VARCHAR, isbn13 VARCHAR);
ALTER TABLE gr_book_ids ADD CONSTRAINT gr_book_id_pk PRIMARY KEY (gr_book_rid);
CREATE UNIQUE INDEX gr_book_id_idx ON gr_book_ids (gr_book_id);
CREATE INDEX gr_book_work_idx ON gr_book_ids (gr_work_id);
CREATE INDEX gr_book_asin_idx ON gr_book_ids (gr_asin);
CREATE INDEX gr_book_isbn_idx ON gr_book_ids (gr_isbn);
CREATE INDEX gr_book_isbn13_idx ON gr_book_ids (gr_isbn13);
ALTER TABLE gr_book_ids ADD CONSTRAINT gr_book_id_fk FOREIGN KEY (gr_book_rid) REFERENCES gr_raw_book (gr_book_rid);
ALTER TABLE gr_book_ids ADD CONSTRAINT gr_book_id_work_fk FOREIGN KEY (gr_work_id) REFERENCES gr_work_ids (gr_work_id);
ANALYZE gr_book_ids;

-- Update ISBN ID records with the ISBNs seen in GoodReads
INSERT INTO isbn_id (isbn)
SELECT DISTINCT gr_isbn FROM gr_book_ids
WHERE gr_isbn IS NOT NULL AND gr_isbn NOT IN (SELECT isbn FROM isbn_id);
INSERT INTO isbn_id (isbn)
SELECT gr_isbn13 FROM gr_book_ids
WHERE gr_isbn13 IS NOT NULL AND gr_isbn13 NOT IN (SELECT isbn FROM isbn_id);

-- Map ISBNs to book IDs
CREATE TABLE gr_book_isbn
  AS SELECT gr_book_id, isbn_id, COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id)) AS book_code
  FROM gr_book_ids, isbn_id
  WHERE isbn = gr_isbn OR isbn = gr_isbn13;
CREATE INDEX gr_book_isbn_book_idx ON gr_book_isbn (gr_book_id);
CREATE INDEX gr_book_isbn_isbn_idx ON gr_book_isbn (isbn_id);
CREATE INDEX gr_book_isbn_code_idx ON gr_book_isbn (book_code);
ALTER TABLE gr_book_isbn ADD CONSTRAINT gr_book_isbn_book_fk FOREIGN KEY (gr_book_id) REFERENCES gr_book_ids (gr_book_id);
ALTER TABLE gr_book_isbn ADD CONSTRAINT gr_book_isbn_isbn_fk FOREIGN KEY (isbn_id) REFERENCES isbn_id (isbn_id);
ANALYZE gr_book_isbn;