DROP TABLE IF EXISTS az_ratings CASCADE;
CREATE TABLE az_ratings (
  user_key VARCHAR NOT NULL,
  asin VARCHAR NOT NULL,
  rating REAL NOT NULL,
  rating_time BIGINT NOT NULL
);
