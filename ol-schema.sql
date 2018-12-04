-- Initial table creation with no constraints or indexes
CREATE SCHEMA IF NOT EXISTS ol;

DROP TABLE IF EXISTS ol.author;
CREATE TABLE ol.author (
    author_id SERIAL NOT NULL,
    author_key VARCHAR(100) NOT NULL,
    author_data JSONB NOT NULL
);

DROP TABLE IF EXISTS ol.work CASCADE;
CREATE TABLE ol.work (
    work_id SERIAL NOT NULL,
    work_key VARCHAR(100) NOT NULL,
    work_data JSONB NOT NULL
);

DROP TABLE IF EXISTS ol.edition CASCADE;
CREATE TABLE ol.edition (
    edition_id SERIAL NOT NULL,
    edition_key VARCHAR(100) NOT NULL,
    edition_data JSONB NOT NULL
);
