CREATE INDEX IF NOT EXISTS bx_rating_user_idx ON bx_ratings (user_id);
CREATE INDEX IF NOT EXISTS bx_rating_isbn_idx ON bx_ratings (isbn);
ANALYZE bx_ratings;

INSERT INTO isbn_bookid (isbn, book_id)
    WITH bad_isbns AS (SELECT DISTINCT isbn
                       FROM bx_ratings br
                       WHERE NOT EXISTS (SELECT * FROM isbn_bookid ib WHERE ib.isbn = br.isbn))
    SELECT isbn, nextval('synthetic_book_id') FROM bad_isbns;
ANALYZE isbn_bookid;

DROP VIEW IF EXISTS bx_explicit_ratings;
CREATE VIEW bx_explicit_ratings
  AS SELECT user_id, book_id, MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx_ratings
       JOIN isbn_bookid USING (isbn)
     WHERE rating > 0
     GROUP BY user_id, book_id;

DROP VIEW IF EXISTS bx_all_ratings;
CREATE VIEW bx_all_ratings
  AS SELECT user_id, book_id, MEDIAN(rating) AS rating, COUNT(rating) AS nratings
     FROM bx_ratings
       JOIN isbn_bookid USING (isbn)
     GROUP BY user_id, book_id;