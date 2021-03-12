--- #dep gr-books
--- #dep gr-works
--- #dep gr-authors
--- #table gr.book_first_author
--- #table gr.author_info

--- #step Extract book first authors
CREATE MATERIALIZED VIEW IF NOT EXISTS gr.book_first_author AS
SELECT gr_book_rid, role AS author_role, author_id AS gr_author_id
FROM gr.raw_book, jsonb_to_record(gr_book_data->'authors'->0) AS
    x(role VARCHAR, author_id INTEGER);
CREATE INDEX IF NOT EXISTS gr_bfa_book_idx ON gr.book_first_author (gr_book_rid);
CREATE INDEX IF NOT EXISTS gr_bfa_auth_idx ON gr.book_first_author (gr_author_id);
ANALYZE gr.book_first_author;

--- #step Extract book authors
CREATE MATERIALIZED VIEW IF NOT EXISTS gr.book_authors AS
SELECT gr_book_rid, role AS author_role, author_id AS gr_author_id
FROM gr.raw_book, jsonb_to_recordset(gr_book_data->'authors') AS
    x(role VARCHAR, author_id INTEGER);
CREATE INDEX IF NOT EXISTS gr_ba_book_idx ON gr.book_authors (gr_book_rid);
CREATE INDEX IF NOT EXISTS gr_ba_auth_idx ON gr.book_authors (gr_author_id);
ANALYZE gr.book_authors;

--- #step Extract author IDs
CREATE TABLE IF NOT EXISTS gr.author_info
  AS SELECT gr_author_rid, (gr_author_data->>'author_id')::int AS gr_author_id,
        gr_author_data->>'name' AS author_name
     FROM gr.raw_author;
CREATE UNIQUE INDEX IF NOT EXISTS gr_author_ridx ON gr.author_info (gr_author_rid);
CREATE UNIQUE INDEX IF NOT EXISTS gr_author_idx ON gr.author_info (gr_author_id);
ANALYZE gr.author_info;
