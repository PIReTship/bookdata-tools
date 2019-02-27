--- #step Index node IRIs
CREATE INDEX IF NOT EXISTS node_iri_idx ON locid.nodes (node_iri);
ANALYZE locid.nodes;

--- #step Add PK to literals
--- #allow invalid_table_definition
ALTER TABLE locid.literals ADD CONSTRAINT literal_pkey PRIMARY KEY (lit_id);
ANALYZE locid.literals;

--- #step Index authority subjects and objects
CREATE INDEX IF NOT EXISTS auth_subject_idx ON locid.auth_triple (subject_id);
CREATE INDEX IF NOT EXISTS auth_object_idx ON locid.auth_triple (object_id);
CLUSTER locid.auth_triple USING auth_subject_idx;
ANALYZE locid.auth_triple;

--- #step Index work subjects and objects
CREATE INDEX IF NOT EXISTS work_subject_idx ON locid.work_triple (subject_id);
CREATE INDEX IF NOT EXISTS work_object_idx ON locid.work_triple (object_id);
CLUSTER locid.work_triple USING work_subject_idx;
ANALYZE locid.work_triple;
