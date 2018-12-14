CREATE INDEX IF NOT EXISTS bx_rating_user_idx ON bx_raw_ratings (user_id);
CREATE INDEX IF NOT EXISTS bx_rating_isbn_idx ON bx_raw_ratings (isbn);
ANALYZE bx_raw_ratings;

INSERT INTO isbn_id (isbn)
  SELECT DISTINCT isbn
  FROM bx_raw_ratings WHERE isbn NOT IN (SELECT isbn FROM isbn_id);
ANALYZE isbn_id;

DROP VIEW IF EXISTS bx_rating;
CREATE VIEW bx_rating
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx_raw_ratings
       JOIN isbn_id USING (isbn)
       LEFT JOIN isbn_cluster USING (isbn_id)
     WHERE rating > 0
     GROUP BY user_id, book_id;

DROP VIEW IF EXISTS bx_add_action;
CREATE VIEW bx_add_action
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     COUNT(rating) AS nactions
     FROM bx_raw_ratings
       JOIN isbn_id USING (isbn)
       LEFT JOIN isbn_cluster USING (isbn_id)
     GROUP BY user_id, book_id;
