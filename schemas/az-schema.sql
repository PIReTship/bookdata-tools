--- #dep common-schema
--- #table az.raw_rating
--- #table az.raw_review
--- #table az.raw_book
CREATE SCHEMA IF NOT EXISTS az14;
CREATE SCHEMA IF NOT EXISTS az18;

DROP TABLE IF EXISTS az14.raw_rating CASCADE;
CREATE TABLE az14.raw_rating (
  user_key VARCHAR NOT NULL,
  asin VARCHAR NOT NULL,
  rating REAL NOT NULL,
  rating_time BIGINT NOT NULL
);

DROP TABLE IF EXISTS az18.raw_review CASCADE;
CREATE TABLE az18.raw_review (
  review_id SERIAL PRIMARY KEY,
  review_data JSONB NOT NULL
);

DROP TABLE IF EXISTS az18.raw_book CASCADE;
CREATE TABLE az18.raw_book (
  book_id SERIAL PRIMARY KEY,
  book_data JSONB NOT NULL
);
