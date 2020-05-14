--- #dep gr-books
--- #dep gr-works
--- #dep gr-index-books
--- #dep cluster
--- #table gr.work_title
--- #table gr.book_pub_date

--- #step Create useful GR functions
CREATE OR REPLACE FUNCTION try_date(year VARCHAR, month VARCHAR, day VARCHAR) RETURNS DATE
IMMUTABLE RETURNS NULL ON NULL INPUT PARALLEL UNSAFE
    LANGUAGE plpgsql AS $$
    BEGIN
        RETURN MAKE_DATE(NULLIF(year, '')::INTEGER,
                    NULLIF(month, '')::INTEGER,
                    NULLIF(day, '')::INTEGER);
    EXCEPTION WHEN SQLSTATE '22008' THEN
        RETURN NULL;
    END;
    $$;

--- #step Index book clusters
CREATE MATERIALIZED VIEW IF NOT EXISTS gr.book_cluster
  AS SELECT DISTINCT gr_book_id, cluster
     FROM gr.book_isbn JOIN isbn_cluster USING (isbn_id);
CREATE UNIQUE INDEX IF NOT EXISTS book_cluster_book_idx ON gr.book_cluster (gr_book_id);
CREATE INDEX IF NOT EXISTS book_cluster_cluster_idx ON gr.book_cluster (cluster);
ANALYZE gr.book_cluster;

--- #step Extract GoodReads work titles
DROP MATERIALIZED VIEW IF EXISTS gr.work_title;
CREATE MATERIALIZED VIEW gr.work_title
AS SELECT gr_work_rid, (gr_work_data->>'work_id')::int AS gr_work_id,
  NULLIF(gr_work_data->>'original_title', '') AS work_title
FROM gr.raw_work;
CREATE INDEX gr_work_title_work_idx ON gr.work_title (gr_work_id);
ANALYZE gr.work_title;

--- #step Extract GoodReads book publication dates
DROP MATERIALIZED VIEW IF EXISTS gr.book_pub_date;
CREATE MATERIALIZED VIEW gr.book_pub_date
AS SELECT gr_book_rid, book_id AS gr_book_id,
          NULLIF(publication_year, '')::INTEGER AS pub_year,
          NULLIF(publication_month, '')::INTEGER AS pub_month,
          NULLIF(publication_day, '')::INTEGER AS pub_day,
          try_date(publication_year, publication_month, publication_day) AS pub_date
   FROM gr.raw_book,
        jsonb_to_record(gr_book_data) AS
            x(book_id INTEGER, publication_year VARCHAR,
              publication_month VARCHAR, publication_day VARCHAR)
   WHERE NULLIF(publication_year, '') IS NOT NULL;
CREATE UNIQUE INDEX gr_bpd_rec_idx ON gr.book_pub_date (gr_book_rid);
CREATE UNIQUE INDEX gr_bpd_book_idx ON gr.book_pub_date (gr_book_id);
ANALYZE gr.book_pub_date;

--- #step Extract GoodReads work original publication dates
DROP MATERIALIZED VIEW IF EXISTS gr.work_pub_date;
CREATE MATERIALIZED VIEW gr.work_pub_date
AS SELECT gr_work_rid, work_id AS gr_work_id,
          NULLIF(original_publication_year, '')::INTEGER AS pub_year,
          NULLIF(original_publication_month, '')::INTEGER AS pub_month,
          NULLIF(original_publication_day, '')::INTEGER AS pub_day,
          try_date(original_publication_year, original_publication_month, original_publication_day) AS pub_date
   FROM gr.raw_work,
        jsonb_to_record(gr_work_data) AS
            x(work_id INTEGER, original_publication_year VARCHAR,
              original_publication_month VARCHAR, original_publication_day VARCHAR)
   WHERE NULLIF(original_publication_year, '') IS NOT NULL;
CREATE UNIQUE INDEX gr_wpd_rec_idx ON gr.work_pub_date (gr_work_rid);
CREATE UNIQUE INDEX gr_wpd_work_idx ON gr.work_pub_date (gr_work_id);
ANALYZE gr.work_pub_date;
