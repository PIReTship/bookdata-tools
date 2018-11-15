DROP TABLE IF EXISTS az_raw_ratings CASCADE;
CREATE TABLE az_raw_ratings (
  user_id INTEGER NOT NULL,
  isbn VARCHAR NOT NULL,
  rating REAL NOT NULL
);
