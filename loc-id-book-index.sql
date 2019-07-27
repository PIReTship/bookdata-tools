--- #step Extract instance IRIs
DROP MATERIALIZED VIEW IF EXISTS locid.instance_entity CASCADE;
CREATE MATERIALIZED VIEW locid.instance_entity AS
SELECT sn.node_id AS instance_id, subject_id AS instance_uuid,
  sn.node_iri AS instance_iri
FROM locid.instance_triples
JOIN locid.nodes sn ON (subject_id = sn.node_uuid)
WHERE pred_id = locid.common_node('type')
  AND object_id = locid.common_node('instance');
CREATE INDEX instance_inst_id_idx ON locid.instance_entity (instance_id);
CREATE INDEX instance_inst_uuid_idx ON locid.instance_entity (instance_uuid);

--- #step Analyze instance IRIs
--- #notx
VACUUM ANALYZE locid.instance_entity;

--- #step Extract work IRIs
DROP MATERIALIZED VIEW IF EXISTS locid.work_entity CASCADE;
CREATE MATERIALIZED VIEW locid.work_entity AS
SELECT sn.node_id AS work_id, subject_id AS work_uuid,
  sn.node_iri AS work_iri
FROM locid.work_triples
JOIN locid.nodes sn ON (subject_id = sn.node_uuid)
WHERE pred_id = locid.common_node('type')
  AND object_id = locid.common_node('work');
CREATE INDEX work_inst_id_idx ON locid.work_entity (work_id);
CREATE INDEX work_inst_uuid_idx ON locid.work_entity (work_uuid);

--- #step Analyze work IRIs
--- #notx
VACUUM ANALYZE locid.work_entity;

--- #step Extract instance/work relationships
DROP MATERIALIZED VIEW IF EXISTS locid.instance_work CASCADE;
CREATE MATERIALIZED VIEW locid.instance_work AS
SELECT DISTINCT isn.node_id as instance_id, isn.node_uuid as instance_uuid,
  wsn.node_id AS work_id, wsn.node_uuid AS work_uuid
FROM locid.instance_triples it
JOIN locid.nodes isn ON (isn.node_uuid = it.subject_id)
JOIN locid.nodes wsn ON (wsn.node_uuid = it.object_id)
JOIN locid.work_entity we ON (it.object_id = we.work_uuid);
CREATE INDEX instance_work_instance_idx ON locid.instance_work (instance_uuid);
CREATE INDEX instance_work_work_idx ON locid.instance_work (work_uuid);

--- #step Index instance ISBNs
DROP MATERIALIZED VIEW IF EXISTS locid.instance_isbn CASCADE;
CREATE MATERIALIZED VIEW locid.instance_isbn AS
SELECT tt.subject_id AS subject_id,
  il.lit_value AS raw_isbn
FROM locid.instance_triples tt
JOIN locid.instance_literals il USING (subject_id)
WHERE
  -- subject is of type ISBN
  tt.pred_id = locid.common_node('type')
  AND tt.object_id = locid.common_node('isbn')
  -- we have a literal value
  AND il.pred_id = locid.common_node('value');
CREATE INDEX instance_isbn_node_idx ON locid.instance_isbn (subject_id);
ANALYZE locid.instance_isbn;

--- #step Index work ISBNs
DROP MATERIALIZED VIEW IF EXISTS locid.work_isbn CASCADE;
CREATE MATERIALIZED VIEW locid.work_isbn AS
SELECT tt.subject_id AS subject_id,
  wl.lit_value AS raw_isbn
FROM locid.work_triples tt
JOIN locid.work_literals wl USING (subject_id)
WHERE
  -- subject is of type ISBN
  tt.pred_id = locid.common_node('type')
  AND tt.object_id = locid.common_node('isbn')
  -- we have a literal value
  AND wl.pred_id = locid.common_node('value');
CREATE INDEX work_isbn_node_idx ON locid.work_isbn (subject_id);
ANALYZE locid.work_isbn;
