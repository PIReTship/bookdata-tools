DO $$
DECLARE
  st_time timestamp;
BEGIN
  RAISE NOTICE 'Indexing node IRIs' USING TABLE = 'locid.nodes';
  st_time := now();
  CREATE INDEX IF NOT EXISTS node_iri_idx ON locid.nodes (node_iri);
  ANALYZE locid.nodes;
  RAISE NOTICE 'Indexed node IRIs in %', now() - st_time;
END;
$$;

DO $$
DECLARE
  st_time timestamp;
BEGIN
  RAISE NOTICE 'Adding literal PK' USING TABLE = 'locid.literals';
  st_time := now();
  ALTER TABLE locid.literals ADD CONSTRAINT literal_pkey PRIMARY KEY (lit_id);
  ANALYZE locid.literals;
  RAISE NOTICE 'Added literal PK in %', now() - st_time;
END;
$$;

DO $$
DECLARE
  st_time timestamp;
BEGIN
  RAISE NOTICE 'Indexing authority subjects and objects' USING TABLE = 'locid.auth_triple';
  st_time := now();
  CREATE INDEX IF NOT EXISTS auth_subject_idx ON locid.auth_triple (subject_id);
  CREATE INDEX IF NOT EXISTS auth_object_idx ON locid.auth_triple (object_id);
  CLUSTER locid.auth_triple ON auth_subject_idx;
  ANALYZE locid.auth_triple;
  RAISE NOTICE 'Indexed authority table in %', now() - st_time;
END;
$$;

DO $$
DECLARE
  st_time timestamp;
BEGIN
  RAISE NOTICE 'Indexing BIBFRAME work subjects and objects' USING TABLE = 'locid.work_triple';
  st_time := now();
  CREATE INDEX IF NOT EXISTS work_subject_idx ON locid.work_triple (subject_id);
  CREATE INDEX IF NOT EXISTS work_object_idx ON locid.work_triple (object_id);
  CLUSTER locid.work_triple ON work_subject_idx;
  ANALYZE locid.work_triple;
  RAISE NOTICE 'Indexed BIBFRAME work table in %', now() - st_time;
END;
$$;
