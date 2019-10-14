--- #dep common-schema
CREATE SCHEMA IF NOT EXISTS locid;

-- We cannot run more than 1 simulatneous import into this schema.
-- For efficiency, the importer will dump tables and bulk-reload them.

--- Nodes
DROP TABLE IF EXISTS locid.nodes;
CREATE TABLE locid.nodes (
    node_id SERIAL PRIMARY KEY,
    node_uuid UUID NOT NULL UNIQUE,
    node_iri VARCHAR
);

--- Authority record triples
DROP TABLE IF EXISTS locid.auth_triples;
CREATE TABLE locid.auth_triples (
    subject_uuid UUID NOT NULL,
    pred_uuid UUID NOT NULL,
    object_uuid UUID NOT NULL
);

DROP TABLE IF EXISTS locid.auth_literals;
CREATE TABLE locid.auth_literals (
    subject_uuid UUID NOT NULL,
    pred_uuid UUID NOT NULL,
    lit_value TEXT NOT NULL,
    lit_lang VARCHAR NULL
);


--- BIBRAME work triples
DROP TABLE IF EXISTS locid.work_triples;
CREATE TABLE locid.work_triples (
    subject_uuid UUID NOT NULL,
    pred_uuid UUID NOT NULL,
    object_uuid UUID NOT NULL
);

DROP TABLE IF EXISTS locid.work_literals;
CREATE TABLE locid.work_literals (
    subject_uuid UUID NOT NULL,
    pred_uuid UUID NOT NULL,
    lit_value TEXT NOT NULL,
    lit_lang VARCHAR NULL
);

--- BIBRAME instance triples
DROP TABLE IF EXISTS locid.instance_triples;
CREATE TABLE locid.instance_triples (
    subject_uuid UUID NOT NULL,
    pred_uuid UUID NOT NULL,
    object_uuid UUID NOT NULL
);

DROP TABLE IF EXISTS locid.instance_literals;
CREATE TABLE locid.instance_literals (
    subject_uuid UUID NOT NULL,
    pred_uuid UUID NOT NULL,
    lit_value TEXT NOT NULL,
    lit_lang VARCHAR NULL
);
