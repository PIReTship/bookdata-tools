--- #dep loc-id-instances

--- #step Index instance subjects and objects
CREATE INDEX IF NOT EXISTS instance_subject_uuidx ON locid.instance_triples (subject_uuid);
CREATE INDEX IF NOT EXISTS instance_object_uuidx ON locid.instance_triples (object_uuid);

--- #step Index instance literal subjects
CREATE INDEX IF NOT EXISTS instance_lit_subject_uuidx ON locid.instance_literals (subject_uuid);

--- #step Analyze instance triples
--- #notx
VACUUM ANALYZE locid.instance_triples;
--- #step Analyze instance literals
--- #notx
VACUUM ANALYZE locid.instance_literals;

--- #step Extract instance node types
DROP MATERIALIZED VIEW IF EXISTS locid.instances_node_type;
CREATE MATERIALIZED VIEW locid.instance_node_type AS
SELECT DISTINCT subject_uuid, object_uuid
FROM locid.instance_triples
WHERE pred_uuid = node_uuid('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
CREATE INDEX int_subj_idx ON locid.instance_node_type (subject_uuid);
CREATE INDEX int_obj_idx ON locid.instance_node_type (object_uuid);

--- #step Analyze instance node types
--- #notx
VACUUM ANALYZE locid.instance_node_type;

--- #step Extract identifiable instance nodes
CREATE MATERIALIZED VIEW IF NOT EXISTS locid.instance_node_triples AS
SELECT sn.node_id AS subject_id, pn.node_id AS pred_id, object_uuid
FROM locid.instance_triples
JOIN locid.nodes sn ON (sn.node_uuid = subject_uuid)
JOIN locid.nodes pn ON (pn.node_uuid = pred_uuid);
CREATE INDEX IF NOT EXISTS instance_node_trip_subject_idx ON locid.instance_node_triples (subject_id);
CREATE INDEX IF NOT EXISTS instance_node_trip_object_idx ON locid.instance_node_triples (object_uuid);
ANALYZE locid.instance_node_triples;
