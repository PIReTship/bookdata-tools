CREATE INDEX viaf_marc_field_rec_idx ON viaf_marc_field (rec_id);

CREATE MATERIALIZED VIEW viaf_author_name
  AS SELECT rec_id, regexp_replace(contents, '\W+$', '') AS name
  FROM viaf_marc_field
  WHERE TAG = '700' AND sf_code = 'a';
CREATE INDEX viaf_author_rec_idx ON viaf_author_name (rec_id);
CREATE INDEX viaf_author_name_idx ON viaf_author_name (name);

CREATE MATERIALIZED VIEW viaf_author_gender
  AS SELECT rec_id, contents AS gender
  FROM viaf_marc_field
  WHERE TAG = '375' AND sf_code = 'a';
CREATE INDEX viaf_gender_rec_idx ON viaf_author_gender (rec_id);

-- CREATE INDEX viaf_author_name_id_idx ON viaf_author_name (viaf_au_id);
-- CREATE INDEX viaf_author_name_idx ON viaf_author_name (viaf_au_name);
-- ALTER TABLE viaf_author_name ADD CONSTRAINT viaf_au_name_fk FOREIGN KEY (viaf_au_id) REFERENCES viaf_author;

-- DELETE FROM viaf_author_name WHERE viaf_au_name_source = 'SYNTH';
-- INSERT INTO viaf_author_name (viaf_au_id, viaf_au_name, viaf_au_name_source, viaf_au_name_dates)
-- SELECT viaf_au_id, regexp_replace(regexp_replace(viaf_au_name, ',$', ''), '^(.*), (.*)', '\2 \1'), 'SYNTH', viaf_au_name_dates
-- FROM viaf_author_name
-- WHERE viaf_au_name LIKE '%,%';

-- CREATE INDEX viaf_gender_id_idx ON viaf_author_gender (viaf_au_id);
-- ALTER TABLE viaf_author_gender ADD CONSTRAINT viaf_au_id_fk FOREIGN KEY (viaf_au_id) REFERENCES viaf_author;
