--- #dep az18-ratings
--- #dep cluster
--- #table az18.user_ids
--- #step Extract review data
DROP MATERIALIZED VIEW IF EXISTS az18.review CASCADE;
CREATE MATERIALIZED VIEW az18.review AS
SELECT reviewerID AS user_key, asin, overall AS rating, summary, reviewText, unixReviewTime AS review_time
FROM az18.raw_review,
    jsonb_to_record(review_data) AS
        x(reviewerID VARCHAR, asin VARCHAR, overall REAL, summary TEXT, reviewText TEXT,
          unixReviewTime INTEGER);
CREATE INDEX az18_review_user_idx ON az18.review (user_key);
CREATE INDEX az18_review_asin_idx ON az18.review (asin);
ANALYZE az18.review;

--- #step Extract user IDs
DROP TABLE IF EXISTS az18.user_ids CASCADE;
CREATE TABLE az18.user_ids (
  user_id SERIAL PRIMARY KEY,
  user_key VARCHAR NOT NULL,
  UNIQUE (user_key)
);
INSERT INTO az18.user_ids (user_key) SELECT DISTINCT user_key FROM az18.review;
ANALYZE az18.user_ids;

--- #step Extract ISBNs
INSERT INTO isbn_id (isbn)
  SELECT DISTINCT asin
  FROM az18.raw_rating WHERE asin NOT IN (SELECT isbn FROM isbn_id);
ANALYZE isbn_id;

--- #step Set up rating view
DROP VIEW IF EXISTS az18.rating;
CREATE VIEW az18.rating
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     MEDIAN(rating) AS rating,
                     (array_agg(rating ORDER BY review_time DESC))[1] AS last_rating,
                     MEDIAN(review_time) AS timestamp,
                     COUNT(rating) AS nratings
     FROM az18.raw_rating
       JOIN az18.user_ids USING (user_key)
       JOIN isbn_id ON (isbn = asin)
       LEFT JOIN isbn_cluster USING (isbn_id)
     GROUP BY user_id, COALESCE(cluster, bc_of_isbn(isbn_id));
