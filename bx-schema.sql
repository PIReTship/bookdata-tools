DROP TABLE IF EXISTS bx_raw_ratings CASCADE;
CREATE TABLE bx_raw_ratings (
  user_id INTEGER NOT NULL,
  isbn VARCHAR NOT NULL,
  rating REAL NOT NULL
);
