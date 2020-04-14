--- #dep az14-ratings
--- #dep cluster
--- #table az14.user_ids
--- #table az14.rating
--- #step Index ratings
CREATE INDEX IF NOT EXISTS az_rating_user_idx ON az14.raw_rating (user_key);
CREATE INDEX IF NOT EXISTS az_rating_asin_idx ON az14.raw_rating (asin);
ANALYZE az14.raw_rating;

--- #step Extract user IDs
DROP TABLE IF EXISTS az14.user_ids CASCADE;
CREATE TABLE az14.user_ids (
  user_id SERIAL PRIMARY KEY,
  user_key VARCHAR NOT NULL,
  UNIQUE (user_key)
);
INSERT INTO az14.user_ids (user_key) SELECT DISTINCT user_key FROM az14.raw_rating;
ANALYZE az14.user_ids;

--- #step Extract ISBNs
INSERT INTO isbn_id (isbn)
  SELECT DISTINCT asin
  FROM az14.raw_rating WHERE asin NOT IN (SELECT isbn FROM isbn_id);
ANALYZE isbn_id;

--- #step Set up rating view
DROP VIEW IF EXISTS az14.rating;
CREATE VIEW az14.rating
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     MEDIAN(rating) AS rating,
                     (array_agg(rating ORDER BY rating_time DESC))[1] AS last_rating,
                     MEDIAN(rating_time) AS timestamp,
                     COUNT(rating) AS nratings
     FROM az14.raw_rating
       JOIN az14.user_ids USING (user_key)
       JOIN isbn_id ON (isbn = asin)
       LEFT JOIN isbn_cluster USING (isbn_id)
     GROUP BY user_id, COALESCE(cluster, bc_of_isbn(isbn_id));
