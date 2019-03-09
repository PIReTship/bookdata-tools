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
DROP TABLE IF EXISTS locid.auth_triple;
CREATE TABLE locid.auth_triple (
    subject_id INTEGER NOT NULL,
    pred_id INTEGER NOT NULL,
    object_id INTEGER NOT NULL
);

DROP TABLE IF EXISTS locid.auth_literal;
CREATE TABLE locid.auth_literal (
    subject_id INTEGER NOT NULL,
    pred_id INTEGER NOT NULL,
    lit_value TEXT NOT NULL,
    lit_lang VARCHAR NOT NULL
);


--- BIBRAME work triples
DROP TABLE IF EXISTS locid.work_triple;
CREATE TABLE locid.work_triple (
    subject_id INTEGER NOT NULL,
    pred_id INTEGER NOT NULL,
    object_id INTEGER NOT NULL
);

DROP TABLE IF EXISTS locid.work_literal;
CREATE TABLE locid.work_literal (
    subject_id INTEGER NOT NULL,
    pred_id INTEGER NOT NULL,
    lit_value TEXT NOT NULL,
    lit_lang VARCHAR NOT NULL
);
