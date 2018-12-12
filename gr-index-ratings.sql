-- Rating data
CREATE TABLE IF NOT EXISTS gr_interaction
AS SELECT gr_int_rid, book_id, user_id, rating, isRead AS is_read
    FROM gr_raw_interaction,
        jsonb_to_record(gr_int_data) AS
            x(book_id INTEGER, user_id VARCHAR, rating INTEGER,
              isRead BOOLEAN);
DO $$
BEGIN
    ALTER TABLE gr_interaction ADD CONSTRAINT gr_interaction_pk PRIMARY KEY (gr_int_rid);
EXCEPTION
    WHEN invalid_table_definition THEN
        RAISE NOTICE 'primary key already exists';
END;
$$;
CREATE INDEX IF NOT EXISTS gr_interaction_book_idx ON gr_interaction (book_id);
CREATE INDEX IF NOT EXISTS gr_interaction_user_idx ON gr_interaction (user_id);
ANALYZE gr_interaction;

-- users
CREATE TABLE IF NOT EXISTS gr_user (
  gr_user_rid SERIAL PRIMARY KEY,
  gr_user_id VARCHAR NOT NULL
);
INSERT INTO gr_user (gr_user_id)
  SELECT DISTINCT user_id
  FROM gr_interaction LEFT JOIN gr_user ON (gr_user_id = user_id)
  WHERE gr_user_rid IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS gr_user_id_idx ON gr_user (gr_user_id);
ANALYZE gr_user;

-- ratings

