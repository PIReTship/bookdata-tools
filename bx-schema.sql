DROP TABLE IF EXISTS bx_ratings CASCADE;
CREATE TABLE bx_ratings (
  user_id INTEGER NOT NULL,
  isbn VARCHAR NOT NULL,
  rating REAL NOT NULL
);

CREATE INDEX bx_rating_user_idx ON bx_ratings (user_id);
CREATE INDEX bx_rating_isbn_idx ON bx_ratings (isbn);
ANALYZE bx_ratings;

INSERT INTO isbn_info (isbn, book_id)
  SELECT isbn, nextval('synthetic_book_id_seq') * 3
  FROM bx_ratings
    LEFT JOIN isbn_info USING (isbn)
  WHERE book_id IS NULL;
REFRESH MATERIALIZED VIEW isbn_book_id;

DROP VIEW IF EXISTS bx_book_info;
CREATE VIEW bx_book_info
  AS SELECT isbn, ib.book_id AS book_id, author_id, author_name
     FROM bx_ratings JOIN isbn_book_id ib USING (isbn)
       LEFT OUTER JOIN (SELECT isbn, (array_remove(array_agg(author_id), NULL))[1] AS author_id
                        FROM bx_ratings JOIN edition_isbn USING (isbn)
                          JOIN edition_first_author USING (edition_id)
                        GROUP BY isbn) auth USING (isbn)
       LEFT OUTER JOIN authors USING (author_id);