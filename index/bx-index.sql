--- #dep bx-ratings
--- #dep cluster
--- #step Index ratings
CREATE INDEX IF NOT EXISTS bx_rating_user_idx ON bx.raw_ratings (user_id);
CREATE INDEX IF NOT EXISTS bx_rating_isbn_idx ON bx.raw_ratings (isbn);
ANALYZE bx.raw_ratings;

--- #step Extract ISBNs
INSERT INTO isbn_id (isbn)
  SELECT DISTINCT isbn
  FROM bx.raw_ratings WHERE isbn NOT IN (SELECT isbn FROM isbn_id);
ANALYZE isbn_id;

--- #step Set up rating views
DROP VIEW IF EXISTS bx.rating;
CREATE VIEW bx.rating
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx.raw_ratings
       JOIN isbn_id USING (isbn)
       LEFT JOIN isbn_cluster USING (isbn_id)
     WHERE rating > 0
     GROUP BY user_id, book_id;

DROP VIEW IF EXISTS bx.add_action;
CREATE VIEW bx.add_action
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     COUNT(rating) AS nactions
     FROM bx.raw_ratings
       JOIN isbn_id USING (isbn)
       LEFT JOIN isbn_cluster USING (isbn_id)
     GROUP BY user_id, book_id;
