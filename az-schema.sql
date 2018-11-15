DROP TABLE IF EXISTS az_raw_ratings CASCADE;
CREATE TABLE az_raw_ratings (
  user_key VARCHAR NOT NULL,
  asin VARCHAR NOT NULL,
  rating REAL NOT NULL,
  rating_time BIGINT NOT NULL
);
