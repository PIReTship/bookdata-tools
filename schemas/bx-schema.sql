CREATE SCHEMA IF NOT EXISTS bx;

DROP TABLE IF EXISTS bx.raw_ratings CASCADE;
CREATE TABLE bx.raw_ratings (
  user_id INTEGER NOT NULL,
  isbn VARCHAR NOT NULL,
  rating REAL NOT NULL
);

INSERT INTO stage_dep (stage_name, dep_name, dep_key)
SELECT 'bx-schema', stage_name, stage_key
FROM stage_status
WHERE stage_name = 'common-schema';
