CREATE INDEX IF NOT EXISTS az_rating_user_idx ON az_ratings (user_key);
CREATE INDEX IF NOT EXISTS az_rating_asin_idx ON az_ratings (asin);
ANALYZE az_ratings;

DROP TABLE IF EXISTS az_users CASCADE;
CREATE TABLE az_users (
  user_id SERIAL PRIMARY KEY,
  user_key VARCHAR NOT NULL,
  UNIQUE (user_key)
);
INSERT INTO az_users (user_key) SELECT DISTINCT user_key FROM az_ratings
ANALYZE az_users;

INSERT INTO isbn_bookid (isbn, book_id)
    WITH bad_isbns AS (SELECT DISTINCT asin
                       FROM az_ratings br
                       WHERE NOT EXISTS (SELECT * FROM isbn_bookid ib WHERE ib.isbn = br.asin))
    SELECT asin, nextval('synthetic_book_id') FROM bad_isbns;
ANALYZE isbn_bookid;

DROP VIEW IF EXISTS az_all_ratings;
CREATE VIEW az_all_ratings
  AS SELECT user_id, book_id, MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM az_ratings
       JOIN az_users USING (user_key)
       JOIN isbn_bookid ON (asin = isbn)
     GROUP BY user_id, book_id;