-- Rating data
CREATE TABLE gr_interaction
AS SELECT gr_int_rid, review_id, book_id, user_id, rating, isRead AS is_read
    FROM gr_raw_interaction,
        jsonb_to_record(gr_int_data) AS
            x(review_id VARCHAR, book_id INTEGER, user_id VARCHAR, rating INTEGER,
              isRead BOOLEAN);

ALTER TABLE gr_interaction ADD CONSTRAINT gr_interaction_pk PRIMARY KEY (gr_int_rid);
CREATE INDEX gr_interaction_book_idx ON gr_interaction (book_id);
CREATE INDEX gr_interaction_user_idx ON gr_interaction (user_id);
ANALYZE gr_interaction;
