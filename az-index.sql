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

INSERT INTO isbn_id (isbn)
  SELECT DISTINCT asin
  FROM az_ratings WHERE asin NOT IN (SELECT isbn FROM isbn_id);
ANALYZE isbn_id;

DROP VIEW IF EXISTS az_ratings;
CREATE VIEW az_ratings
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM az_ratings
       JOIN az_users USING (user_key)
       JOIN isbn_id ON (isbn = asin)
       LEFT JOIN isbn_cluster USING (isbn_id)
     GROUP BY user_id, COALESCE(cluster, bc_of_isbn(isbn_id));
