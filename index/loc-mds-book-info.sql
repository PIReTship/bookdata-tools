--- #dep loc-mds-index-books
-- Extract more book information

--- #step Extract authors
CREATE MATERIALIZED VIEW IF NOT EXISTS locmds.book_author_name
  AS SELECT rec_id, regexp_replace(contents, '\W+$', '') AS name
  FROM locmds.book_marc_field
  WHERE tag = '100' AND sf_code = 'a'
  WITH NO DATA;
REFRESH MATERIALIZED VIEW locmds.book_author_name;
CREATE INDEX IF NOT EXISTS book_author_name_rec_idx ON locmds.book_author_name (rec_id);
CREATE INDEX IF NOT EXISTS book_author_name_name_idx ON locmds.book_author_name (name);
ANALYZE locmds.book_author_name;

--- #step Extract publication years
CREATE MATERIALIZED VIEW IF NOT EXISTS locmds.book_pub_year
  AS SELECT rec_id, substring(contents from '(\d\d\d\d)') AS pub_year
  FROM locmds.book_marc_field
  WHERE tag = '260' AND sf_code = 'c' AND substring(contents from '(\d\d\d\d)') IS NOT NULL
  WITH NO DATA;
REFRESH MATERIALIZED VIEW locmds.book_pub_year;
CREATE INDEX IF NOT EXISTS book_pub_year_rec_idx ON locmds.book_pub_year (rec_id);
ANALYZE locmds.book_pub_year;

--- #step Extract book titles
DROP MATERIALIZED VIEW IF EXISTS locmds.book_title;
CREATE MATERIALIZED VIEW locmds.book_title
AS SELECT rec_id, contents AS title
    FROM locmds.book_marc_field
    WHERE tag = '245' AND sf_code = 'a';
CREATE INDEX locmds_book_title_rec_ids ON locmds.book_title (rec_id);
ANALYZE locmds.book_title;
