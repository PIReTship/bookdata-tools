CREATE INDEX IF NOT EXISTS bx_rating_user_idx ON bx_ratings (user_id);
CREATE INDEX IF NOT EXISTS bx_rating_isbn_idx ON bx_ratings (isbn);
ANALYZE bx_ratings;

INSERT INTO isbn_id (isbn)
  SELECT DISTINCT isbn
  FROM bx_ratings WHERE isbn NOT IN (SELECT isbn FROM isbn_id);
ANALYZE isbn_id;

INSERT INTO loc_isbn_book_id (isbn, book_id)
    WITH bad_isbns AS (SELECT DISTINCT isbn
                       FROM bx_ratings br
                       WHERE NOT EXISTS (SELECT * FROM loc_isbn_book_id ib WHERE ib.isbn = br.isbn))
    SELECT isbn, nextval('loc_synthetic_book_id') FROM bad_isbns;
ANALYZE loc_isbn_book_id;

DROP VIEW IF EXISTS bx_loc_explicit_ratings;
CREATE VIEW bx_loc_explicit_ratings
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
       MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx_ratings
       JOIN isbn_id USING (isbn)
       LEFT JOIN loc_isbn_cluster USING (isbn_id)
     WHERE rating > 0
     GROUP BY user_id, book_id;

DROP VIEW IF EXISTS bx_loc_all_ratings;
CREATE VIEW bx_loc_all_ratings
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
       MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx_ratings
       JOIN isbn_id USING (isbn)
       LEFT JOIN loc_isbn_cluster USING (isbn_id)
     GROUP BY user_id, book_id;

DROP VIEW IF EXISTS bx_explicit_ratings;
CREATE VIEW bx_explicit_ratings
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx_ratings
       JOIN isbn_id USING (isbn)
       LEFT JOIN isbn_cluster USING (isbn_id)
     WHERE rating > 0
     GROUP BY user_id, book_id;

DROP VIEW IF EXISTS bx_all_ratings;
CREATE VIEW bx_all_ratings
  AS SELECT user_id, COALESCE(cluster, bc_of_isbn(isbn_id)) AS book_id,
                     MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx_ratings
       JOIN isbn_id USING (isbn)
       LEFT JOIN isbn_cluster USING (isbn_id)
     GROUP BY user_id, book_id;
