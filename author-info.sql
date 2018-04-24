--- Schema for consolidating and calibrating author gender info

CREATE OR REPLACE FUNCTION merge_gender(cgender VARCHAR, ngender VARCHAR) RETURNS VARCHAR AS $$
BEGIN
  RETURN CASE
         WHEN ngender = 'unknown' OR ngender IS NULL THEN cgender
         WHEN cgender = 'unknown' THEN ngender
         WHEN cgender = ngender THEN ngender
         ELSE 'ambiguous'
         END;
END;
$$ LANGUAGE plpgsql;

CREATE AGGREGATE resolve_gender(gender VARCHAR) (
  SFUNC = merge_gender,
  STYPE = VARCHAR,
  INITCOND = 'unknown'
);

DROP TABLE IF EXISTS loc_book_author_gender;
CREATE TABLE loc_book_author_gender
  AS SELECT book_id,
       case when count(an.name) = 0 then 'no-loc-author'
         when count(vn.rec_id) = 0 then 'no-viaf-author'
         when count(vg.gender) = 0 then 'no-gender'
         else resolve_gender(vg.gender)
       end AS gender
     FROM loc_isbn_book_id
       JOIN loc_isbn USING (isbn)
       LEFT JOIN loc_author_name an USING (rec_id)
       LEFT JOIN viaf_author_name vn USING (name)
       LEFT JOIN viaf_author_gender vg ON (vn.rec_id = vg.rec_id)
     GROUP BY book_id;
CREATE UNIQUE INDEX loc_book_author_gender_book_idx ON loc_book_author_gender (book_id);