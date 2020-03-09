--- #dep common-schema
--- #table az.raw_ratings
CREATE SCHEMA IF NOT EXISTS az;

DROP TABLE IF EXISTS az.raw_ratings CASCADE;
CREATE TABLE az.raw_ratings (
  user_key VARCHAR NOT NULL,
  asin VARCHAR NOT NULL,
  rating REAL NOT NULL,
  rating_time BIGINT NOT NULL
);
