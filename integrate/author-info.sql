--- #dep bx-index
--- #dep az-index
--- #dep gr-index-ratings
--- #dep cluster
--- #dep loc-mds-cluster
--- #dep loc-id-author-index
--- #dep viaf-index

--- Schema for consolidating and calibrating author gender info
--- #step Create functions
CREATE OR REPLACE FUNCTION merge_gender(cgender VARCHAR, ngender VARCHAR) RETURNS VARCHAR
IMMUTABLE STRICT PARALLEL SAFE
AS $$ SELECT CASE
             WHEN ngender = 'unknown' OR ngender IS NULL THEN cgender
             WHEN cgender = 'unknown' THEN ngender
             WHEN cgender = ngender THEN ngender
             ELSE 'ambiguous'
             END
$$ LANGUAGE SQL;

DROP AGGREGATE IF EXISTS resolve_gender(VARCHAR);
CREATE AGGREGATE resolve_gender(gender VARCHAR) (
  SFUNC = merge_gender,
  STYPE = VARCHAR,
  INITCOND = 'unknown'
);

--- #step Extract book first-author names from OL
DROP MATERIALIZED VIEW IF EXISTS cluster_ol_first_author_name CASCADE;
CREATE MATERIALIZED VIEW cluster_ol_first_author_name AS
  SELECT DISTINCT cluster, author_name
  FROM isbn_cluster
    JOIN ol.isbn_link USING (isbn_id)
    JOIN ol.edition USING (edition_id)
    JOIN ol.edition_first_author USING (edition_id)
    JOIN ol.author_name USING (author_id);

--- #step Extract book first-author names from LOC MDS
DROP MATERIALIZED VIEW IF EXISTS cluster_loc_first_author_name CASCADE;
CREATE MATERIALIZED VIEW cluster_loc_first_author_name AS
  SELECT DISTINCT cluster, name AS author_name
  FROM isbn_cluster
    JOIN locmds.book_rec_isbn USING (isbn_id)
    JOIN locmds.book_author_name USING (rec_id);

--- #step Extract book first-author names from all available book-author data
DROP MATERIALIZED VIEW IF EXISTS cluster_first_author_name CASCADE;
CREATE MATERIALIZED VIEW cluster_first_author_name AS
  SELECT cluster, author_name FROM cluster_loc_first_author_name
  UNION DISTINCT
  SELECT cluster, author_name FROM cluster_ol_first_author_name;
CREATE INDEX IF NOT EXISTS cluster_first_author_name_cluster_idx ON cluster_first_author_name (cluster);
CREATE INDEX IF NOT EXISTS cluster_first_author_name_idx ON cluster_first_author_name (author_name);
ANALYZE cluster_first_author_name;

--- #step Compute genders of first authors form all available VIAF data
CREATE TABLE IF NOT EXISTS viaf.cluster_first_author_gender (
  cluster INTEGER NOT NULL,
  gender VARCHAR NOT NULL
);
TRUNCATE viaf.cluster_first_author_gender;
INSERT INTO viaf.cluster_first_author_gender
  SELECT cluster,
    case
    when count(an.author_name) = 0 then 'no-loc-author'
    when count(vn.rec_id) = 0 then 'no-viaf-author'
    when count(vg.gender) = 0 then 'no-gender'
    else resolve_gender(vg.gender)
    end AS gender
  FROM (SELECT DISTINCT cluster FROM isbn_cluster WHERE cluster < bc_of_isbn(0)) cl -- ISBN-only recs aren't useful
    LEFT JOIN cluster_first_author_name an USING (cluster)
    LEFT JOIN viaf.author_name vn ON (name = author_name)
    LEFT JOIN viaf.author_gender vg ON (vn.rec_id = vg.rec_id)
  GROUP BY cluster;
CREATE UNIQUE INDEX IF NOT EXISTS cluster_first_author_gender_book_idx ON viaf.cluster_first_author_gender (cluster);
ANALYZE viaf.cluster_first_author_gender;

--- #step Compute genders of first authors form all available LOCID data
CREATE TABLE IF NOT EXISTS locid.cluster_first_author_gender (
  cluster INTEGER NOT NULL,
  gender VARCHAR NOT NULL
);
TRUNCATE locid.cluster_first_author_gender;
INSERT INTO locid.cluster_first_author_gender
  SELECT cluster,
    case
    when count(an.author_name) = 0 then 'no-book-author'
    when count(lan.auth_uuid) = 0 then 'no-author-rec'
    when count(ag.gender_uuid) = 0 then 'no-gender'
    else resolve_gender(gc.code)
    end AS gender
  FROM (SELECT DISTINCT cluster FROM isbn_cluster WHERE cluster < bc_of_isbn(0)) cl -- ISBN-only recs aren't useful
    LEFT JOIN cluster_first_author_name an USING (cluster)
    LEFT JOIN locid.auth_name lan ON (name = author_name)
    LEFT JOIN locid.auth_gender ag ON (lan.auth_uuid = ag.auth_uuid)
    LEFT JOIN locid.gender_codes gc USING (gender_uuid)
  GROUP BY cluster;
CREATE UNIQUE INDEX IF NOT EXISTS cluster_first_author_gender_book_idx ON locid.cluster_first_author_gender (cluster);
ANALYZE locid.cluster_first_author_gender;

--- #step Save stage deps
INSERT INTO stage_dep (stage_name, dep_name, dep_key)
SELECT 'author-info', stage_name, stage_key
FROM stage_status
WHERE stage_name IN ('cluster', 'loc-mds-cluster');
