CREATE INDEX IF NOT EXISTS az_rating_user_idx ON az_ratings (user_key);
CREATE INDEX IF NOT EXISTS az_rating_asin_idx ON az_ratings (asin);
ANALYZE az_ratings;

DROP TABLE IF EXISTS az_users CASCADE;
CREATE TABLE az_users (
  user_id SERIAL PRIMARY KEY,
  user_key VARCHAR NOT NULL,
  UNIQUE (user_key)
);
INSERT INTO az_users (user_key) SELECT DISTINCT user_key FROM az_ratings;
ANALYZE az_users;

INSERT INTO isbn_info (isbn, book_id)
  SELECT asin, nextval('synthetic_book_id_seq') * 3
  FROM az_ratings
    LEFT JOIN isbn_info ON (asin = isbn)
  WHERE book_id IS NULL;
REFRESH MATERIALIZED VIEW isbn_book_id;
ANALYZE isbn_info;
ANALYZE isbn_book_id;
REFRESH MATERIALIZED VIEW ol_book_first_author;
ANALYZE ol_book_first_author;

DROP VIEW IF EXISTS az_book_info;
CREATE VIEW az_book_info
  AS SELECT DISTINCT ib.book_id AS book_id, asin, author_id, author_name, author_gender
     FROM az_ratings JOIN isbn_book_id ib ON (asin = isbn)
       LEFT OUTER JOIN ol_book_first_author USING (book_id)
       LEFT OUTER JOIN ol_author USING (author_id)
       LEFT OUTER JOIN author_resolution USING (author_id);

DROP VIEW IF EXISTS az_export_ratings;
CREATE VIEW az_export_ratings
  AS SELECT user_id, book_id, MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM az_ratings
       JOIN az_users USING (user_key)
       JOIN isbn_book_id ON (asin = isbn)
     GROUP BY user_id, book_id;