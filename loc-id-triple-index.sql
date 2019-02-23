BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

CREATE INDEX IF NOT EXISTS node_iri_idx ON locid.nodes (node_iri);

ALTER TABLE locid.literals ADD CONSTRAINT literal_pkey PRIMARY KEY (lit_id);

CREATE INDEX IF NOT EXISTS auth_subject_idx ON locid.auth_triple (subject_id);
CREATE INDEX IF NOT EXISTS auth_object_idx ON locid.auth_triple (object_id);

CREATE INDEX IF NOT EXISTS work_subject_idx ON locid.work_triple (subject_id);
CREATE INDEX IF NOT EXISTS work_object_idx ON locid.work_triple (object_id);

COMMIT;
