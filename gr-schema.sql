CREATE SCHEMA IF NOT EXISTS gr;

CREATE TABLE gr.raw_interaction (
  gr_int_rid SERIAL NOT NULL,
  gr_int_data JSONB NOT NULL
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
