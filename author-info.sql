--- Schema for consolidating and calibrating author gender info

CREATE OR REPLACE FUNCTION merge_gender(cgender VARCHAR, ngender VARCHAR) RETURNS VARCHAR
IMMUTABLE STRICT PARALLEL SAFE
AS $$ SELECT CASE
             WHEN ngender = 'unknown' OR ngender IS NULL THEN cgender
             WHEN cgender = 'unknown' THEN ngender
             WHEN cgender = ngender THEN ngender
             ELSE 'ambiguous'
             END
$$ LANGUAGE SQL;

CREATE AGGREGATE resolve_gender(gender VARCHAR) (
  SFUNC = merge_gender,
  STYPE = VARCHAR,
  INITCOND = 'unknown'
);

DROP TABLE IF EXISTS loc_cluster_author_gender;
CREATE TABLE loc_cluster_author_gender
  AS SELECT cluster,
       case when count(an.name) = 0 then 'no-loc-author'
         when count(vn.rec_id) = 0 then 'no-viaf-author'
         when count(vg.gender) = 0 then 'no-gender'
         else resolve_gender(vg.gender)
       end AS gender
     FROM loc_isbn_cluster
       JOIN loc_rec_isbn USING (isbn_id)
       LEFT JOIN loc_author_name an USING (rec_id)
       LEFT JOIN viaf_author_name vn USING (name)
       LEFT JOIN viaf_author_gender vg ON (vn.rec_id = vg.rec_id)
     GROUP BY cluster;
CREATE UNIQUE INDEX loc_cluster_author_gender_book_idx ON loc_cluster_author_gender (cluster);

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
       JOIN loc_rec_isbn ri USING (isbn_id)
       LEFT JOIN loc_author_name an USING (rec_id)
       LEFT JOIN viaf_author_name vn USING (name)
       LEFT JOIN viaf_author_gender vg ON (vn.rec_id = vg.rec_id)
     GROUP BY cluster;
CREATE UNIQUE INDEX cluster_author_gender_book_idx ON cluster_loc_author_gender (cluster);

DROP MATERIALIZED VIEW IF EXISTS rated_book CASCADE;
CREATE MATERIALIZED VIEW rated_book AS
SELECT DISTINCT cluster, isbn_id
  FROM (SELECT book_id AS cluster FROM bx_all_ratings
        UNION DISTINCT
        SELECT book_id AS cluster FROM az_export_ratings) rated
  LEFT JOIN isbn_cluster USING (cluster);
CREATE INDEX rated_book_cluster_idx ON rated_book (cluster);
CREATE INDEX rated_book_isbn_idx ON rated_book (isbn_id);
ANALYZE rated_book;

CREATE MATERIALIZED VIEW cluster_author_name AS
  SELECT cluster, author_name
  FROM isbn_cluster
    JOIN ol_isbn_link USING (isbn_id)
    JOIN ol_edition USING (edition_id)
    JOIN ol_edition_author USING (edition_id)
    JOIN ol_author_name USING (author_id)
  UNION DISTINCT
  SELECT cluster, name
  FROM isbn_cluster
    JOIN loc_rec_isbn USING (isbn_id)
    JOIN loc_author_name USING (rec_id);
CREATE INDEX cluster_author_name_cluster_idx ON cluster_author_name (cluster);
CREATE INDEX cluster_author_name_idx ON cluster_author_name (author_name);
ANALYZE cluster_author_name;

DROP TABLE IF EXISTS cluster_author_gender;
CREATE TABLE cluster_author_gender
  AS SELECT cluster,
       case
       when count(an.author_name) = 0 then 'no-loc-author'
       when count(vn.rec_id) = 0 then 'no-viaf-author'
       when count(vg.gender) = 0 then 'no-gender'
       else resolve_gender(vg.gender)
       end AS gender
     FROM (SELECT DISTINCT cluster FROM isbn_cluster WHERE cluster < bc_of_isbn(0)) cl
       LEFT JOIN cluster_author_name an USING (cluster)
       LEFT JOIN viaf_author_name vn ON (name = author_name)
       LEFT JOIN viaf_author_gender vg ON (vn.rec_id = vg.rec_id)
     GROUP BY cluster;
CREATE UNIQUE INDEX cluster_author_gender_book_idx ON cluster_author_gender (cluster);