-- Initial table creation with no constraints or indexes
DROP TABLE IF EXISTS authors;
CREATE TABLE authors (
    author_id SERIAL NOT NULL,
    author_key VARCHAR(100) NOT NULL,
    author_name VARCHAR,
    author_data JSONB NOT NULL
);

DROP TABLE IF EXISTS works CASCADE;
CREATE TABLE works (
    work_id SERIAL NOT NULL,
    work_key VARCHAR(100) NOT NULL,
    work_title VARCHAR,
    work_data JSONB NOT NULL
);

DROP TABLE IF EXISTS editions CASCADE;
CREATE TABLE editions (
    edition_id SERIAL NOT NULL,
    edition_key VARCHAR(100) NOT NULL,
    edition_title VARCHAR,
    edition_data JSONB NOT NULL
);

-- Create indexes and constraints
ALTER TABLE authors ADD PRIMARY KEY (author_id);
ALTER TABLE authors ADD CONSTRAINT author_key_uq UNIQUE (author_key);
ALTER TABLE works ADD PRIMARY KEY (work_id);
ALTER TABLE works ADD CONSTRAINT work_key_uq UNIQUE (work_key);
ALTER TABLE editions ADD PRIMARY KEY (edition_id);
ALTER TABLE editions ADD CONSTRAINT edition_key_uq UNIQUE (edition_key);

-- Set up work-author join table
DROP TABLE IF EXISTS work_authors CASCADE;
CREATE TABLE work_authors (
    work_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL
);

INSERT INTO work_authors (work_id, author_id)
SELECT work_id, author_id
FROM authors
JOIN (SELECT work_id, jsonb_array_elements((work_data->'authors')) #>> '{author,key}' AS author_key FROM works) w
    USING (author_key);

CREATE INDEX work_author_wk_idx ON work_authors (work_id);
CREATE INDEX work_author_au_idx ON work_authors (author_id);
ALTER TABLE work_authors ADD CONSTRAINT work_author_wk_fk FOREIGN KEY (work_id) REFERENCES works;
ALTER TABLE work_authors ADD CONSTRAINT work_author_au_fk FOREIGN KEY (author_id) REFERENCES authors;

-- Set up edition-author join table
DROP TABLE IF EXISTS edition_authors CASCADE;
CREATE TABLE edition_authors (
    edition_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL
);

INSERT INTO edition_authors (edition_id, author_id)
SELECT edition_id, author_id
FROM authors
JOIN (SELECT edition_id, jsonb_array_elements((edition_data->'authors')) ->> 'key' AS author_key FROM editions) w
    USING (author_key);

CREATE INDEX edition_author_ed_idx ON edition_authors (edition_id);
CREATE INDEX edition_author_au_idx ON edition_authors (author_id);
ALTER TABLE edition_authors ADD CONSTRAINT edition_authors_ed_fk FOREIGN KEY (edition_id) REFERENCES editions;
ALTER TABLE edition_authors ADD CONSTRAINT edition_authors_au_fk FOREIGN KEY (author_id) REFERENCES authors;

ANALYZE;