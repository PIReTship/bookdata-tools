CREATE TABLE gr_interaction_raw (
  gr_int_rid SERIAL NOT NULL,
  gr_int_data JSONB NOT NULL
);
CREATE TABLE gr_book_raw (
  gr_book_rid SERIAL NOT NULL,
  gr_book_data JSONB NOT NULL
);
CREATE TABLE gr_work_raw (
  gr_work_rid SERIAL NOT NULL,
  gr_work_data JSONB NOT NULL
);
CREATE TABLE gr_author_raw (
  gr_author_rid SERIAL NOT NULL,
  gr_author_data JSONB NOT NULL
);
