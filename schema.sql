-- Initial table creation with no constraints or indexes
DROP TABLE IF EXISTS authors;
CREATE TABLE authors (
    author_id SERIAL NOT NULL,
    author_key VARCHAR(32) NOT NULL,
    author_name VARCHAR,
    author_data JSONB NOT NULL
);

DROP TABLE IF EXISTS works;
CREATE TABLE works (
    work_id SERIAL NOT NULL,
    work_key VARCHAR(32) NOT NULL,
    work_title VARCHAR,
    work_data JSONB NOT NULL
);

DROP TABLE IF EXISTS editions;
CREATE TABLE editions (
    edition_id SERIAL NOT NULL,
    edition_key VARCHAR(32) NOT NULL,
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
ANALYZE authors;
ANALYZE works;
ANALYZE editions;