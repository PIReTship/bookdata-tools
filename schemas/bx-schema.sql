--- #dep common-schema
--- #table bx.raw_ratings
CREATE SCHEMA IF NOT EXISTS bx;

DROP TABLE IF EXISTS bx.raw_ratings CASCADE;
CREATE TABLE bx.raw_ratings (
  user_id INTEGER NOT NULL,
  isbn VARCHAR NOT NULL,
  rating REAL NOT NULL
);
