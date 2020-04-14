--- #dep common-schema
--- #table bx.raw_rating
CREATE SCHEMA IF NOT EXISTS bx;

DROP TABLE IF EXISTS bx.raw_rating CASCADE;
CREATE TABLE bx.raw_rating (
  user_id INTEGER NOT NULL,
  isbn VARCHAR NOT NULL,
  rating REAL NOT NULL
);
