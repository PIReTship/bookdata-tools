CREATE SCHEMA IF NOT EXISTS locid;

-- We cannot run more than 1 simulatneous import into this schema.
-- For efficiency, the importer will dump tables and bulk-reload them.

--- Nodes
DROP TABLE IF EXISTS locid.nodes;
CREATE TABLE locid.nodes (
    node_id UUID PRIMARY KEY,
    node_iri VARCHAR NOT NULL
);

--- Literals
DROP TABLE IF EXISTS locid.literals;
CREATE TABLE locid.literals (
    lit_id UUID NOT NULL,
    lit_value TEXT NOT NULL
);

--- Authority record triples
DROP TABLE IF EXISTS locid.auth_triple;
CREATE TABLE locid.auth_triple (
    subject_id UUID NOT NULL, -- REFERENCES nodes
    pred_id UUID NOT NULL, -- REFERENCES nodes
    object_id UUID NOT NULL -- either a node or a literal
);

--- BIBRAME work triples
DROP TABLE IF EXISTS locid.work_triple;
CREATE TABLE locid.work_triple (
    subject_id UUID NOT NULL, -- REFERENCES nodes
    pred_id UUID NOT NULL, -- REFERENCES nodes
    object_id UUID NOT NULL -- either a node or a literal
);
