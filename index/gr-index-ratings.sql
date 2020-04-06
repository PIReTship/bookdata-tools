--- #dep gr-interactions
--- #dep cluster
--- #table gr.user_info
--- #table gr.interaction
--- #table gr.rating
--- #step Add interaction PK
--- #allow invalid_table_definition
ALTER TABLE gr.raw_interaction ADD CONSTRAINT gr_raw_interaction_pk PRIMARY KEY (gr_interaction_rid);

--- #step Extract user info
CREATE TABLE IF NOT EXISTS gr.user_info (
  gr_user_rid SERIAL PRIMARY KEY,
  gr_user_id VARCHAR NOT NULL
);
CREATE TEMPORARY TABLE gr_new_users
  AS SELECT gr_interaction_data->>'user_id' AS gr_user_id
     FROM gr.raw_interaction LEFT JOIN gr.user_info ON (gr_user_id = gr_interaction_data->>'user_id')
     WHERE gr_user_rid IS NULL;
INSERT INTO gr.user_info (gr_user_id)
SELECT DISTINCT gr_user_id FROM gr_new_users;

CREATE UNIQUE INDEX IF NOT EXISTS user_id_idx ON gr.user_info (gr_user_id);
ANALYZE gr.user_info;

--- #step Extract interaction data
CREATE TABLE IF NOT EXISTS gr.interaction
  AS SELECT gr_interaction_rid, book_id AS gr_book_id, gr_user_rid, rating, (gr_interaction_data->'isRead')::boolean AS is_read, date_added, date_updated
     FROM gr.raw_interaction,
          jsonb_to_record(gr_interaction_data) AS
              x(book_id INTEGER, user_id VARCHAR, rating INTEGER,
                date_added TIMESTAMP WITH TIME ZONE, date_updated TIMESTAMP WITH TIME ZONE),
          gr.user_info
     WHERE user_id = gr_user_id;
DROP VIEW IF EXISTS gr.work_interaction;
CREATE VIEW gr.work_interaction
AS SELECT ix.*, bid.gr_work_id
FROM gr.interaction ix
LEFT JOIN gr.book_ids bid USING (gr_book_id);

--- #step Index interaction data
--- #allow invalid_table_definition
-- if this step is already done, first ALTER TABLE will fail
ALTER TABLE gr.interaction ADD CONSTRAINT gr_interaction_pk PRIMARY KEY (gr_interaction_rid);
CREATE INDEX IF NOT EXISTS interaction_book_idx ON gr.interaction (gr_book_id);
CREATE INDEX IF NOT EXISTS interaction_user_idx ON gr.interaction (gr_user_rid);
ALTER TABLE gr.interaction ADD CONSTRAINT gr_interaction_book_fk FOREIGN KEY (gr_book_id) REFERENCES gr.book_ids (gr_book_id);
ALTER TABLE gr.interaction ADD CONSTRAINT gr_interaction_user_fk FOREIGN KEY (gr_user_rid) REFERENCES gr.user_info (gr_user_rid);
ANALYZE gr.interaction;

--- #step Extract ratings
CREATE MATERIALIZED VIEW IF NOT EXISTS gr.rating
  AS SELECT gr_user_rid AS user_id, cluster AS book_id,
            MEDIAN(rating) AS rating,
            (array_agg(rating ORDER BY date_updated DESC))[1] AS last_rating,
            MEDIAN(EXTRACT(EPOCH FROM date_updated)) AS timestamp,
            COUNT(rating) AS nratings
     FROM gr.interaction
            JOIN gr.book_cluster USING (gr_book_id)
     WHERE rating > 0
     GROUP BY gr_user_rid, cluster;
CREATE INDEX IF NOT EXISTS rating_user_idx ON gr.rating (user_id);
CREATE INDEX IF NOT EXISTS rating_item_idx ON gr.rating (book_id);
ANALYZE gr.rating;

--- #step Extract add actions
CREATE MATERIALIZED VIEW IF NOT EXISTS gr.add_action
  AS SELECT gr_user_rid AS user_id, cluster AS book_id,
            COUNT(rating) AS nactions,
            MIN(EXTRACT(EPOCH FROM date_updated)) AS first_time,
            MAX(EXTRACT(EPOCH FROM date_updated)) AS last_time
     FROM gr.interaction
            JOIN gr.book_cluster USING (gr_book_id)
     GROUP BY gr_user_rid, cluster;
CREATE INDEX IF NOT EXISTS add_action_user_idx ON gr.add_action (user_id);
CREATE INDEX IF NOT EXISTS add_action_item_idx ON gr.add_action (book_id);
ANALYZE gr.add_action;
