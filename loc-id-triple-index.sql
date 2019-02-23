BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DO $$ RAISE NOTICE 'Indexing node IRIs' USING TABLE = 'locid.nodes'; $$;
CREATE INDEX IF NOT EXISTS node_iri_idx ON locid.nodes (node_iri);
ANALYZE locid.nodes;

DO $$ RAISE NOTICE 'Adding literal PK' USING TABLE = 'locid.literals'; $$;
ALTER TABLE locid.literals ADD CONSTRAINT literal_pkey PRIMARY KEY (lit_id);
ANALYZE locid.literals;

DO $$ RAISE NOTICE 'Indexing authority subjects and objects' USING TABLE = 'locid.auth_triple'; $$;
CREATE INDEX IF NOT EXISTS auth_subject_idx ON locid.auth_triple (subject_id);
CREATE INDEX IF NOT EXISTS auth_object_idx ON locid.auth_triple (object_id);

DO $$ RAISE NOTICE 'Indexing BIBFRAME work subjects and objects' USING TABLE = 'locid.work_triple'; $$;
CREATE INDEX IF NOT EXISTS work_subject_idx ON locid.work_triple (subject_id);
CREATE INDEX IF NOT EXISTS work_object_idx ON locid.work_triple (object_id);

COMMIT;
