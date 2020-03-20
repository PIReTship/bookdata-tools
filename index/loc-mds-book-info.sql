--- #dep loc-mds-index-books
-- Extract more book information

--- #step Extract book titles
DROP MATERIALIZED VIEW IF EXISTS locmds.book_title;
CREATE MATERIALIZED VIEW locmds.book_title
AS SELECT rec_id, contents AS title
    FROM locmds.book_marc_field
    WHERE tag = '245' AND sf_code = 'a';
CREATE INDEX locmds_book_title_rec_ids ON locmds.book_title (rec_id);
ANALYZE locmds.book_title;
