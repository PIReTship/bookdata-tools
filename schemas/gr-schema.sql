--- #dep common-schema
--- #table gr.raw_interaction
--- #table gr.raw_book
--- #table gr.raw_work
--- #table gr.raw_author
--- #table gr.raw_series
--- #table gr.raw_book_genres

DROP SCHEMA IF EXISTS gr CASCADE;
CREATE SCHEMA gr;

CREATE TABLE gr.raw_interaction (
  gr_interaction_rid SERIAL NOT NULL,
  gr_interaction_data JSONB NOT NULL
);
CREATE TABLE gr.raw_book (
  gr_book_rid SERIAL NOT NULL,
  gr_book_data JSONB NOT NULL
);
CREATE TABLE gr.raw_work (
  gr_work_rid SERIAL NOT NULL,
  gr_work_data JSONB NOT NULL
);
CREATE TABLE gr.raw_author (
  gr_author_rid SERIAL NOT NULL,
  gr_author_data JSONB NOT NULL
);
CREATE TABLE gr.raw_series (
  gr_series_rid SERIAL NOT NULL,
  gr_series_data JSONB NOT NULL
);
CREATE TABLE gr.raw_book_genres (
  gr_book_genres_rid SERIAL NOT NULL,
  gr_book_genres_data JSONB NOT NULL
);
