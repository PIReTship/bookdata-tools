--- Schema for consolidating and calibrating author gender info

DROP MATERIALIZED VIEW IF EXISTS rated_authors;
CREATE MATERIALIZED VIEW rated_authors AS
  SELECT author_id, COUNT(distinct book_id) AS num_books
  FROM (SELECT book_id, author_id FROM az_book_info WHERE author_id IS NOT NULL
        UNION SELECT book_id, author_id FROM bx_book_info WHERE author_id IS NOT NULL) bids
  GROUP BY author_id;
CREATE INDEX rated_authors_auth_idx ON rated_authors (author_id);
ANALYZE rated_authors;

DROP MATERIALIZED VIEW IF EXISTS author_resolution_summary;
CREATE MATERIALIZED VIEW author_resolution_summary AS
WITH res_stats AS (SELECT author_id, author_name,
                     COUNT(distinct viaf_au_id) AS au_count,
                     COUNT(distinct NULLIF(viaf_au_gender, 'unknown')) AS gender_count
                   FROM rated_authors
                     JOIN authors USING (author_id)
                     LEFT OUTER JOIN viaf_author_name ON (viaf_au_name = author_name)
                     LEFT OUTER JOIN viaf_author_gender USING (viaf_au_id)
                   GROUP BY author_id, author_name)
  SELECT author_id, author_name, au_count, gender_count,
    CASE WHEN au_count = 0 THEN 'unlinked'
    WHEN gender_count = 0 THEN 'uncoded'
    WHEN gender_count = 1 THEN 'resolved'
    WHEN gender_count = 2 THEN 'ambiguous'
    ELSE NULL
    END AS status
  FROM res_stats;
CREATE INDEX au_res_author_idx ON author_resolution_summary (author_id);
ANALYZE author_resolution_summary;
