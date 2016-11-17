
-- Initial table creation with no constraints or indexes
CREATE TABLE authors (
    author_id SERIAL NOT NULL,
    author_key VARCHAR(32) NOT NULL,
    author_name VARCHAR,
    author_data JSONB NOT NULL
);

CREATE TABLE works (
    work_id SERIAL NOT NULL,
    work_key VARCHAR(32) NOT NULL,
    work_title VARCHAR,
    work_data JSONB NOT NULL
);

CREATE TABLE editions (
    edition_id SERIAL NOT NULL,
    edition_key VARCHAR(32) NOT NULL,
    edition_title VARCHAR,
    edition_data JSONB NOT NULL
);

-- Scratch tables for importing relationships
CREATE TABLE edition_works_tmp (
    edition_key VARCHAR(32) NOT NULL,
    work_key VARCHAR(32) NOT NULL
);

CREATE TABLE edition_authors_tmp (
    edition_key VARCHAR(32) NOT NULL,
    author_key VARCHAR(32) NOT NULL
);

CREATE TABLE work_authors_tmp (
    work_key VARCHAR(32) NOT NULL,
    author_key VARCHAR(32) NOT NULL
);

-- Create indexes and constraints
ALTER TABLE authors ADD PRIMARY KEY (author_id);
ALTER TABLE authors ADD CONSTRAINT author_key_uq UNIQUE (author_key);
ALTER TABLE works ADD PRIMARY KEY (work_id);
ALTER TABLE works ADD CONSTRAINT work_key_uq UNIQUE (work_key);
ALTER TABLE editions ADD PRIMARY KEY (edition_id);
ALTER TABLE editions ADD CONSTRAINT edition_key_uq UNIQUE (edition_key);
ANALYZE authors;
ANALYZE works;
ANALYZE editions;

-- Set up real join tables
CREATE TABLE edition_works (
    edition_id INTEGER NOT NULL,
    work_id INTEGER NOT NULL
);
INSERT INTO edition_works
    SELECT edition_id, work_id
    FROM edition_works_tmp JOIN editions USING (edition_key)
    JOIN works USING (work_key);
CREATE INDEX edition_works_edition_idx ON edition_works (edition_id);
CREATE INDEX edition_works_work_idx ON edition_works (work_id);
ALTER TABLE edition_works ADD CONSTRAINT edition_works_ed_fk
FOREIGN KEY (edition_id) REFERENCES editions;
ALTER TABLE edition_works ADD CONSTRAINT edition_works_work_fk
FOREIGN KEY (work_id) REFERENCES works;
ANALYZE edition_works;

CREATE TABLE edition_authors (
    edition_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL
);
INSERT INTO edition_authors
    SELECT edition_id, author_id
    FROM edition_authors_tmp JOIN editions USING (edition_key)
    JOIN authors USING (author_key);
CREATE INDEX edition_authors_edition_idx ON edition_authors (edition_id);
CREATE INDEX edition_authors_author_idx ON edition_authors (author_id);
ALTER TABLE edition_authors ADD CONSTRAINT edition_authors_ed_fk
FOREIGN KEY (edition_id) REFERENCES editions;
ALTER TABLE edition_authors ADD CONSTRAINT edition_authors_author_fk
FOREIGN KEY (author_id) REFERENCES authors;
ANALYZE edition_authors;

CREATE TABLE work_authors (
    work_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL
);
INSERT INTO work_authors
    SELECT work_id, author_id
    FROM work_authors_tmp JOIN works USING (work_key)
    JOIN authors USING (author_key);
CREATE INDEX work_authors_work_idx ON work_authors (work_id);
CREATE INDEX work_authors_author_idx ON work_authors (author_id);
ALTER TABLE work_authors ADD CONSTRAINT work_authors_work_fk
FOREIGN KEY (work_id) REFERENCES works;
ALTER TABLE work_authors ADD CONSTRAINT work_authors_author_fk
FOREIGN KEY (author_id) REFERENCES authors;

DROP TABLE work_authors_tmp;
DROP TABLE edition_works_tmp;
DROP TABLE edition_authors_tmp;