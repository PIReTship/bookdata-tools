CREATE SCHEMA IF NOT EXISTS locid;

-- We cannot run more than 1 simulatneous import into this schema.
-- For efficiency, the importer will dump tables and bulk-reload them.

--- Nodes
CREATE TABLE locid.nodes (
    node_id BIGINT NOT NULL, -- always positive
    node_iri VARCHAR NOT NULL
);

--- Literals
CREATE TABLE locid.literals (
    lit_id BIGINT NOT NULL, -- always negative
    lit_value TEXT NOT NULL
);

--- Authority record triples
CREATE TABLE locid.auth_triple (
    subject_id BIGINT NOT NULL, -- REFERENCES nodes
    pred_id BIGINT NOT NULL, -- REFERENCES nodes
    object_id BIGINT NOT NULL -- either a node or a literal
);

--- BIBRAME work triples
CREATE TABLE locid.work_triple (
    subject_id BIGINT NOT NULL, -- REFERENCES nodes
    pred_id BIGINT NOT NULL, -- REFERENCES nodes
    object_id BIGINT NOT NULL -- either a node or a literal
);
