--- #dep loc-id-works

--- #step Index work subjects and objects
CREATE INDEX IF NOT EXISTS work_subject_uuidx ON locid.work_triples (subject_uuid);
CREATE INDEX IF NOT EXISTS work_object_uuidx ON locid.work_triples (object_uuid);

--- #step Index work literal subjects
CREATE INDEX IF NOT EXISTS work_lit_subject_uuidx ON locid.work_literals (subject_uuid);

--- #step Analyze work triples
--- #notx
VACUUM ANALYZE locid.work_triples;
--- #step Analyze work literals
--- #notx
VACUUM ANALYZE locid.work_literals;

--- #step Extract work node types
DROP MATERIALIZED VIEW IF EXISTS locid.work_node_type;
CREATE MATERIALIZED VIEW locid.work_node_type AS
SELECT DISTINCT subject_uuid, object_uuid
FROM locid.work_triples
WHERE pred_uuid = node_uuid('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
CREATE INDEX work_subj_idx ON locid.work_node_type (subject_uuid);

--- #step Analyze work node types
--- #notx
VACUUM ANALYZE locid.work_node_type;

--- #step Extract identifiable work nodes
CREATE MATERIALIZED VIEW IF NOT EXISTS locid.work_node_triples AS
SELECT sn.node_id AS subject_id, pn.node_id AS pred_id, object_uuid
FROM locid.work_triples
JOIN locid.nodes sn ON (sn.node_uuid = subject_uuid)
JOIN locid.nodes pn ON (pn.node_uuid = pred_uuid);
CREATE INDEX IF NOT EXISTS work_node_trip_subject_idx ON locid.work_node_triples (subject_id);
CREATE INDEX IF NOT EXISTS work_node_trip_object_idx ON locid.work_node_triples (object_uuid);
ANALYZE locid.work_node_triples;
