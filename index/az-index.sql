--- #step Index ratings
CREATE INDEX IF NOT EXISTS az_rating_user_idx ON az.raw_ratings (user_key);
CREATE INDEX IF NOT EXISTS az_rating_asin_idx ON az.raw_ratings (asin);
ANALYZE az.raw_ratings;

--- #step Extract user IDs
DROP TABLE IF EXISTS az.user_ids CASCADE;
CREATE TABLE az.user_ids (
  user_id SERIAL PRIMARY KEY,
  user_key VARCHAR NOT NULL,
  UNIQUE (user_key)
);
INSERT INTO az.user_ids (user_key) SELECT DISTINCT user_key FROM az.raw_ratings;
ANALYZE az.user_ids;

--- #step Extract ISBNs
INSERT INTO isbn_id (isbn)
  SELECT DISTINCT asin
  FROM az.raw_ratings WHERE asin NOT IN (SELECT isbn FROM isbn_id);
ANALYZE isbn_id;

--- #step Set up rating view
DROP VIEW IF EXISTS az.rating;
CREATE VIEW az.rating
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     MEDIAN(rating) AS rating,
                     (array_agg(rating ORDER BY rating_time DESC))[1] AS last_rating,
                     MEDIAN(rating_time) AS timestamp,
                     COUNT(rating) AS nratings
     FROM az.raw_ratings
       JOIN az.user_ids USING (user_key)
       JOIN isbn_id ON (isbn = asin)
       LEFT JOIN isbn_cluster USING (isbn_id)
     GROUP BY user_id, COALESCE(cluster, bc_of_isbn(isbn_id));

--- #step Save stage deps
INSERT INTO stage_dep (stage_name, dep_name, dep_key)
SELECT 'az-index', stage_name, stage_key
FROM stage_status
WHERE stage_name IN = 'az-ratings';