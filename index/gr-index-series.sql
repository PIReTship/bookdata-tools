--- #dep gr-books
--- #dep gr-series
--- #dep gr-index-books
--- #table gr.book_series

--- #step Extract book series IDs
DROP TABLE IF EXISTS gr.series_ids CASCADE;
CREATE TABLE gr.series_ids
AS SELECT gr_series_rid, gr_series_data->>'series_id' AS series_id
   FROM gr.raw_series;
ALTER TABLE gr.series_ids ADD PRIMARY KEY (gr_series_rid);
CREATE UNIQUE INDEX series_id_uq ON gr.series_ids (series_id);
ANALYZE gr.series_ids;

--- #step Extract series associations from books
DROP TABLE IF EXISTS gr.book_series CASCADE;
CREATE TABLE gr.book_series
AS SELECT gr_book_rid, (gr_book_data->>'book_id')::integer AS gr_book_id,
    jsonb_array_elements_text(gr_book_data->'series') AS series_id
   FROM gr.raw_book;
CREATE INDEX book_series_book_ridx ON gr.book_series (gr_book_rid);
CREATE INDEX book_series_book_idx ON gr.book_series (gr_book_id);
CREATE INDEX book_series_series_idx ON gr.book_series (series_id);
ANALYZE gr.book_series;
