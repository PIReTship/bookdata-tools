-- index book clusters since those can't be done before indexing
CREATE MATERIALIZED VIEW IF NOT EXISTS gr.book_cluster
  AS SELECT DISTINCT gr_book_id, cluster
     FROM gr_book_isbn JOIN isbn_cluster USING (isbn_id);
CREATE UNIQUE INDEX IF NOT EXISTS gr.book_cluster_book_idx ON gr.book_cluster (gr_book_id);
CREATE INDEX IF NOT EXISTS gr.book_cluster_cluster_idx ON gr.book_cluster (cluster);
ANALYZE gr.book_cluster;

-- users
CREATE TABLE IF NOT EXISTS gr.user_info (
  gr_user_rid SERIAL PRIMARY KEY,
  gr_user_id VARCHAR NOT NULL
);
CREATE TEMPORARY TABLE gr_new_users
  AS SELECT gr_interaction_data->>'user_id' AS gr_user_id
     FROM gr_raw_interaction LEFT JOIN gr.user_info ON (gr_user_id = gr_interaction_data->>'user_id')
     WHERE gr_user_rid IS NULL;
INSERT INTO gr.user_info (gr_user_id)
SELECT DISTINCT gr_user_id FROM gr_new_users;

CREATE UNIQUE INDEX IF NOT EXISTS gr.user_id_idx ON gr.user_info (gr_user_id);
ANALYZE gr.user_info;

-- Rating data
CREATE TABLE IF NOT EXISTS gr.interaction
  AS SELECT gr_interaction_rid, book_id, gr_user_rid, rating, (gr_interaction_data->'isRead')::boolean AS is_read, date_added, date_updated
     FROM gr_raw_interaction,
          jsonb_to_record(gr_interaction_data) AS
              x(book_id INTEGER, user_id VARCHAR, rating INTEGER,
                date_added TIMESTAMP WITH TIME ZONE, date_updated TIMESTAMP WITH TIME ZONE),
          gr.user_info
     WHERE user_id = gr_user_id;
DO $$
BEGIN
    ALTER TABLE gr.interaction ADD CONSTRAINT gr_interaction_pk PRIMARY KEY (gr_interaction_rid);
EXCEPTION
    WHEN invalid_table_definition THEN
        RAISE NOTICE 'primary key already exists';
END;
$$;
CREATE INDEX IF NOT EXISTS gr.interaction_book_idx ON gr.interaction (book_id);
CREATE INDEX IF NOT EXISTS gr.interaction_user_idx ON gr.interaction (gr_user_rid);
DO $$
BEGIN
    ALTER TABLE gr.interaction ADD CONSTRAINT gr_interaction_book_fk FOREIGN KEY (book_id) REFERENCES gr_book_ids (gr_book_id);
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'book FK already exists';
END;
$$;
DO $$
BEGIN
    ALTER TABLE gr.interaction ADD CONSTRAINT gr_interaction_user_fk FOREIGN KEY (gr_user_rid) REFERENCES gr.user_info (gr_user_rid);
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'user FK already exists';
END;
$$;
ANALYZE gr.interaction;

-- ratings
CREATE MATERIALIZED VIEW IF NOT EXISTS gr.rating
  AS SELECT gr_user_rid AS user_id, cluster AS book_id,
            MEDIAN(rating) AS med_rating,
            (array_agg(rating ORDER BY date_updated DESC))[1] AS last_rating,
            COUNT(rating) AS nratings
     FROM gr.interaction
            JOIN gr.book_cluster ON (gr_book_id = book_id)
     WHERE rating > 0
     GROUP BY gr_user_rid, cluster;
CREATE INDEX IF NOT EXISTS gr.rating_user_idx ON gr.rating (user_id);
CREATE INDEX IF NOT EXISTS gr.rating_item_idx ON gr.rating (book_id);
ANALYZE gr.rating;

CREATE MATERIALIZED VIEW IF NOT EXISTS gr.add_action
  AS SELECT gr_user_rid AS user_id, cluster AS book_id,
            COUNT(rating) AS nactions
     FROM gr.interaction
            JOIN gr.book_cluster ON (gr_book_id = book_id)
     GROUP BY gr_user_rid, cluster;
CREATE INDEX IF NOT EXISTS gr.add_action_user_idx ON gr.add_action (user_id);
CREATE INDEX IF NOT EXISTS gr.add_action_item_idx ON gr.add_action (book_id);
ANALYZE gr.add_action;
