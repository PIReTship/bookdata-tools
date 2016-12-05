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