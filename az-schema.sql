DROP TABLE IF EXISTS az_ratings CASCADE;
CREATE TABLE az_ratings (
  user_key VARCHAR NOT NULL,
  asin VARCHAR NOT NULL,
  rating REAL NOT NULL,
  rating_time BIGINT NOT NULL
);

CREATE INDEX az_rating_user_idx ON az_ratings (user_key);
CREATE INDEX az_rating_asin_idx ON az_ratings (asin);
ANALYZE az_ratings;

INSERT INTO isbn_info (isbn, book_id)
  SELECT asin, nextval('synthetic_book_id_seq') * 3
  FROM az_ratings
    LEFT JOIN isbn_info ON (asin = isbn)
  WHERE book_id IS NULL;
REFRESH MATERIALIZED VIEW isbn_book_id;

DROP VIEW IF EXISTS az_book_info;
CREATE VIEW az_book_info
  AS SELECT asin, ib.book_id AS book_id, author_id, author_name
     FROM az_ratings JOIN isbn_book_id ib ON (asin = isbn)
       LEFT OUTER JOIN (SELECT isbn, (array_remove(array_agg(author_id), NULL))[1] AS author_id
                        FROM az_ratings JOIN edition_isbn ON (isbn = asin)
                          JOIN edition_first_author USING (edition_id)
                        GROUP BY isbn) auth USING (isbn)
       LEFT OUTER JOIN authors USING (author_id);