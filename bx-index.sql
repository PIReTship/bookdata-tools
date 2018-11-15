CREATE INDEX IF NOT EXISTS bx_rating_user_idx ON bx_ratings (user_id);
CREATE INDEX IF NOT EXISTS bx_rating_isbn_idx ON bx_ratings (isbn);
ANALYZE bx_ratings;

INSERT INTO isbn_id (isbn)
  SELECT DISTINCT isbn
  FROM bx_ratings WHERE isbn NOT IN (SELECT isbn FROM isbn_id);
ANALYZE isbn_id;

DROP VIEW IF EXISTS bx_explicit_ratings;
CREATE VIEW bx_explicit_ratings
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx_ratings
       JOIN isbn_id USING (isbn)
       LEFT JOIN isbn_cluster USING (isbn_id)
     WHERE rating > 0
     GROUP BY user_id, book_id;

DROP VIEW IF EXISTS bx_ratings;
CREATE VIEW bx_ratings
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx_ratings
       JOIN isbn_id USING (isbn)
       LEFT JOIN isbn_cluster USING (isbn_id)
     GROUP BY user_id, book_id;
