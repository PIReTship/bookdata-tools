DROP TABLE IF EXISTS bx_ratings CASCADE;
CREATE TABLE bx_ratings (
  user_id INTEGER NOT NULL,
  isbn VARCHAR NOT NULL,
  rating REAL NOT NULL
);

CREATE INDEX bx_rating_user_idx ON bx_ratings (user_id);
CREATE INDEX bx_rating_isbn_idx ON bx_ratings (isbn);
ANALYZE bx_ratings;