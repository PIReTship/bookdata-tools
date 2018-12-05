-- Initial table creation with no constraints or indexes
DROP TABLE IF EXISTS ol_author;
CREATE TABLE ol_author (
    author_id SERIAL NOT NULL,
    author_key VARCHAR(100) NOT NULL,
    author_data JSONB NOT NULL
);

DROP TABLE IF EXISTS ol_work CASCADE;
CREATE TABLE ol_work (
    work_id SERIAL NOT NULL,
    work_key VARCHAR(100) NOT NULL,
    work_data JSONB NOT NULL
);

DROP TABLE IF EXISTS ol_edition CASCADE;
CREATE TABLE ol_edition (
    edition_id SERIAL NOT NULL,
    edition_key VARCHAR(100) NOT NULL,
    edition_data JSONB NOT NULL
);
