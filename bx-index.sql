CREATE INDEX IF NOT EXISTS bx_rating_user_idx ON bx_ratings (user_id);
CREATE INDEX IF NOT EXISTS bx_rating_isbn_idx ON bx_ratings (isbn);
ANALYZE bx_ratings;

INSERT INTO isbn_info (isbn, book_id)
  SELECT isbn, nextval('synthetic_book_id_seq') * 3
  FROM bx_ratings
    LEFT JOIN isbn_info USING (isbn)
  WHERE book_id IS NULL;
REFRESH MATERIALIZED VIEW isbn_book_id;

DROP VIEW IF EXISTS bx_book_info;
CREATE VIEW bx_book_info
  AS SELECT DISTINCT isbn, ib.book_id AS book_id, author_id, author_name
     FROM bx_ratings JOIN isbn_book_id ib USING (isbn)
       LEFT OUTER JOIN (SELECT isbn, (array_remove(array_agg(author_id), NULL))[1] AS author_id
                        FROM bx_ratings JOIN ol_edition_isbn USING (isbn)
                          JOIN ol_edition_first_author USING (edition_id)
                        GROUP BY isbn) auth USING (isbn)
       LEFT OUTER JOIN ol_author USING (author_id);

DROP MATERIALIZED VIEW IF EXISTS bx_explicit_ratings;
CREATE MATERIALIZED VIEW bx_explicit_ratings
  AS SELECT user_id, book_id, MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx_ratings
       JOIN isbn_book_id USING (isbn)
     WHERE rating > 0
     GROUP BY user_id, book_id;

DROP MATERIALIZED VIEW IF EXISTS bx_all_ratings;
CREATE MATERIALIZED VIEW bx_all_ratings
  AS SELECT user_id, book_id, MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx_ratings
       JOIN isbn_book_id USING (isbn)
     GROUP BY user_id, book_id;