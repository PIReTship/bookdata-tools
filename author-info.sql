--- Schema for consolidating and calibrating author gender info

CREATE MATERIALIZED VIEW rated_authors AS
  SELECT author_id, COUNT(distinct book_id) AS num_books
  FROM (SELECT book_id, author_id FROM az_book_info WHERE author_id IS NOT NULL
        UNION SELECT book_id, author_id FROM bx_book_info WHERE author_id IS NOT NULL) bids
  GROUP BY author_id;
CREATE INDEX rated_authors_auth_idx ON rated_authors (author_id);