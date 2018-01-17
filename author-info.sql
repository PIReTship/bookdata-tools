--- Schema for consolidating and calibrating author gender info

DROP MATERIALIZED VIEW IF EXISTS rated_authors;
CREATE MATERIALIZED VIEW rated_authors AS
  SELECT author_id, COUNT(distinct book_id) AS num_books
  FROM (SELECT DISTINCT book_id FROM az_ratings JOIN isbn_book_id ib ON (asin = isbn)
        UNION SELECT DISTINCT book_id FROM bx_ratings JOIN isbn_book_id ib USING (isbn)) bids
    JOIN ol_book_first_author USING (book_id)
  WHERE author_id IS NOT NULL
  GROUP BY author_id;
CREATE INDEX rated_authors_auth_idx ON rated_authors (author_id);
ANALYZE rated_authors;

CREATE OR REPLACE FUNCTION merge_gender(cgender VARCHAR, ngender VARCHAR) RETURNS VARCHAR AS $$
BEGIN
  RETURN CASE
         WHEN ngender = 'unknown' OR ngender IS NULL THEN cgender
         WHEN cgender = 'unknown' THEN ngender
         WHEN cgender = ngender THEN ngender
         ELSE 'ambiguous'
         END;
END;
$$ LANGUAGE plpgsql;

CREATE AGGREGATE resolve_gender(gender VARCHAR) (
  SFUNC = merge_gender,
  STYPE = VARCHAR,
  INITCOND = 'unknown'
);

DROP MATERIALIZED VIEW IF EXISTS author_resolution_summary;
CREATE MATERIALIZED VIEW author_resolution_summary AS
WITH res_stats AS (SELECT author_id, author_name,
                     COUNT(distinct viaf_au_id) AS au_count,
                     COUNT(distinct NULLIF(viaf_au_gender, 'unknown')) AS gender_count
                   FROM rated_authors
                     JOIN ol_author USING (author_id)
                     LEFT OUTER JOIN viaf_author_name ON (viaf_au_name = author_name)
                     LEFT OUTER JOIN viaf_author_gender USING (viaf_au_id)
                   GROUP BY author_id, author_name)
  SELECT author_id, author_name, au_count, gender_count,
    CASE WHEN au_count = 0 THEN 'no-author'
    WHEN gender_count = 0 THEN 'no-gender'
    WHEN gender_count = 1 THEN 'known'
    WHEN gender_count = 2 THEN 'ambiguous'
    ELSE NULL
    END AS status
  FROM res_stats;
CREATE INDEX au_res_author_idx ON author_resolution_summary (author_id);
ANALYZE author_resolution_summary;

DROP MATERIALIZED VIEW IF EXISTS author_resolution;
CREATE MATERIALIZED VIEW author_resolution AS
  SELECT author_id, author_name, resolve_gender(viaf_au_gender) AS author_gender
  FROM rated_authors
  JOIN ol_author USING (author_id)
  LEFT OUTER JOIN viaf_author_name ON (viaf_au_name = author_name)
  LEFT OUTER JOIN viaf_author_gender USING (viaf_au_id)
  GROUP BY author_id, author_name;
CREATE INDEX au_res_au_idx ON author_resolution (author_id);
ANALYZE author_resolution;

DROP VIEW IF EXISTS rated_book_author;
CREATE VIEW rated_book_author
  AS SELECT book_id, author_id, author_name, author_gender
  FROM ol_book_first_author JOIN author_resolution USING (author_id);