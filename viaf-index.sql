CREATE INDEX viaf_author_id_idx ON viaf_author (viaf_au_id);
CREATE INDEX viaf_author_name_idx ON viaf_author (viaf_au_name);
ALTER TABLE viaf_author_name ADD FOREIGN KEY viaf_au_id REFERENCES viaf_author;

CREATE INDEX viaf_gender_id_idx ON viaf_gender (viaf_au_id);
ALTER TABLE viaf_author_gender ADD FOREIGN KEY viaf_au_id REFERENCES viaf_author;