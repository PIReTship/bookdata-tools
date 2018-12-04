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

--- #step Compute author genders for LOC clusters
DROP TABLE IF EXISTS locmds.cluster_author_gender;
CREATE TABLE locmds.cluster_author_gender
  AS SELECT cluster,
       case when count(an.name) = 0 then 'no-loc-author'
         when count(vn.rec_id) = 0 then 'no-viaf-author'
         when count(vg.gender) = 0 then 'no-gender'
         else resolve_gender(vg.gender)
       end AS gender
     FROM locmds.isbn_cluster
       JOIN locmds.book_rec_isbn USING (isbn_id)
       LEFT JOIN locmds.book_author_name an USING (rec_id)
       LEFT JOIN viaf.author_name vn USING (name)
       LEFT JOIN viaf.author_gender vg ON (vn.rec_id = vg.rec_id)
     GROUP BY cluster;
CREATE UNIQUE INDEX loc_cluster_author_gender_book_idx ON locmds.cluster_author_gender (cluster);

--- #step Compute author genders for global clusters, using LOC author records
DROP TABLE IF EXISTS cluster_loc_author_gender;
CREATE TABLE cluster_loc_author_gender
  AS SELECT cluster,
       case
       when count(an.name) = 0 then 'no-loc-author'
       when count(vn.rec_id) = 0 then 'no-viaf-author'
       when count(vg.gender) = 0 then 'no-gender'
       else resolve_gender(vg.gender)
       end AS gender
     FROM isbn_cluster
       JOIN locmds.book_rec_isbn ri USING (isbn_id)
       LEFT JOIN locmds.book_author_name an USING (rec_id)
       LEFT JOIN viaf.author_name vn USING (name)
       LEFT JOIN viaf.author_gender vg ON (vn.rec_id = vg.rec_id)
     GROUP BY cluster;
CREATE UNIQUE INDEX cluster_loc_author_gender_book_idx ON cluster_loc_author_gender (cluster);

--- #step Create MV of rated books
DROP MATERIALIZED VIEW IF EXISTS rated_book CASCADE;
CREATE MATERIALIZED VIEW rated_book AS
SELECT DISTINCT cluster, isbn_id
  FROM (SELECT book_id AS cluster FROM bx.add_action
        UNION DISTINCT
        SELECT book_id AS cluster FROM az.rating
        UNION DISTINCT
        SELECT book_id AS cluster FROM gr.add_action) rated
  LEFT JOIN isbn_cluster USING (cluster);
CREATE INDEX rated_book_cluster_idx ON rated_book (cluster);
CREATE INDEX rated_book_isbn_idx ON rated_book (isbn_id);
ANALYZE rated_book;

--- #step Extract book author names from OL
DROP MATERIALIZED VIEW IF EXISTS cluster_ol_author_name;
CREATE MATERIALIZED VIEW cluster_ol_author_name AS
  SELECT DISTINCT cluster, author_name
  FROM isbn_cluster
    JOIN ol.isbn_link USING (isbn_id)
    JOIN ol.edition USING (edition_id)
    JOIN ol.edition_author USING (edition_id)
    JOIN ol.author_name USING (author_id);

--- #step Extract book first-author names from OL
DROP MATERIALIZED VIEW IF EXISTS cluster_ol_first_author_name;
CREATE MATERIALIZED VIEW cluster_ol_first_author_name AS
  SELECT DISTINCT cluster, author_name
  FROM isbn_cluster
    JOIN ol.isbn_link USING (isbn_id)
    JOIN ol.edition USING (edition_id)
    JOIN ol.edition_first_author USING (edition_id)
    JOIN ol.author_name USING (author_id);

--- #step Extract book first-author names from LOC MDS
DROP MATERIALIZED VIEW IF EXISTS cluster_loc_first_author_name;
CREATE MATERIALIZED VIEW cluster_loc_author_name AS
  SELECT DISTINCT cluster, name AS author_name
  FROM isbn_cluster
    JOIN locmds.book_rec_isbn USING (isbn_id)
    JOIN locmds.book_author_name USING (rec_id);

--- #step Extract book first-author names from all available book-author data
DROP MATERIALIZED VIEW IF EXISTS cluster_first_author_name;
CREATE MATERIALIZED VIEW cluster_first_author_name AS
  SELECT cluster, author_name FROM cluster_loc_first_author_name
  UNION DISTINCT
  SELECT cluster, author_name FROM cluster_ol_first_author_name;
CREATE INDEX cluster_first_author_name_cluster_idx ON cluster_first_author_name (cluster);
CREATE INDEX cluster_first_author_name_idx ON cluster_first_author_name (author_name);
ANALYZE cluster_first_author_name;

--- #step Compute genders of first authors form all available data
DROP TABLE IF EXISTS cluster_first_author_gender;
CREATE TABLE cluster_first_author_gender
  AS SELECT cluster,
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
CREATE UNIQUE INDEX cluster_first_author_gender_book_idx ON cluster_first_author_gender (cluster);
ANALYZE cluster_first_author_gender;
