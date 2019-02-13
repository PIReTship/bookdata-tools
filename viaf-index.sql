CREATE INDEX viaf.marc_field_rec_idx ON viaf.marc_field (rec_id);

CREATE VIEW viaf.record_codes
  AS SELECT rec_id,
       SUBSTR(contents, 6, 1) AS status,
       SUBSTR(contents, 7, 1) AS rec_type,
       substr(CONTENTS, 8, 1) AS bib_level
  FROM viaf.marc_field WHERE tag = 'LDR';
CREATE MATERIALIZED VIEW viaf.marc_cn
  AS SELECT rec_id, trim(contents) AS control
  FROM viaf.marc_field
  WHERE tag = '001';
CREATE INDEX viaf.marc_cn_rec_idx ON viaf.marc_cn (rec_id);
ANALYZE viaf.marc_cn;
CREATE MATERIALIZED VIEW viaf.rec_isbn
AS SELECT rec_id, TRIM(contents) AS rec_isbn
   FROM viaf.marc_field WHERE tag = '901' AND sf_code = 'a';
CREATE INDEX viaf.isbn_rec_idx ON viaf.rec_isbn (rec_id);
CREATE INDEX viaf.isbn_isbn_idx ON viaf.rec_isbn (rec_isbn);

DROP TABLE IF EXISTS viaf.author_name CASCADE;
CREATE TABLE viaf.author_name (
  rec_id INTEGER NOT NULL,
  ind VARCHAR(1) NOT NULL,
  name VARCHAR NOT NULL
);
INSERT INTO viaf.author_name
  SELECT rec_id, ind1, regexp_replace(contents, '\W+$', '') AS name
  FROM viaf.marc_field
  WHERE TAG = '700' AND sf_code = 'a';
CREATE INDEX viaf.author_rec_idx ON viaf.author_name (rec_id);
CREATE INDEX viaf.author_name_idx ON viaf.author_name (name);
INSERT INTO viaf.author_name
  SELECT rec_id, 'S', regexp_replace(name, '^(.*), (.*)', '\2 \1')
  FROM viaf.author_name
  WHERE ind = '1';

CREATE MATERIALIZED VIEW viaf.author_gender
  AS SELECT rec_id, contents AS gender
  FROM viaf.marc_field
  WHERE TAG = '375' AND sf_code = 'a';
CREATE INDEX viaf.gender_rec_idx ON viaf.author_gender (rec_id);

-- CREATE INDEX viaf_author_name_id_idx ON viaf.author_name (viaf_au_id);
-- CREATE INDEX viaf_author_name_idx ON viaf.author_name (viaf_au_name);
-- ALTER TABLE viaf.author_name ADD CONSTRAINT viaf_au_name_fk FOREIGN KEY (viaf_au_id) REFERENCES viaf_author;

-- DELETE FROM viaf.author_name WHERE viaf_au_name_source = 'SYNTH';
-- INSERT INTO viaf.author_name (viaf_au_id, viaf_au_name, viaf_au_name_source, viaf_au_name_dates)
-- SELECT viaf_au_id, regexp_replace(regexp_replace(viaf_au_name, ',$', ''), '^(.*), (.*)', '\2 \1'), 'SYNTH', viaf_au_name_dates
-- FROM viaf.author_name
-- WHERE viaf_au_name LIKE '%,%';

-- CREATE INDEX viaf_gender_id_idx ON viaf_author_gender (viaf_au_id);
-- ALTER TABLE viaf_author_gender ADD CONSTRAINT viaf_au_id_fk FOREIGN KEY (viaf_au_id) REFERENCES viaf_author;
