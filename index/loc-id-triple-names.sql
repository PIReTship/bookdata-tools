--- #dep loc-id-names
--- #dep loc-id-triple-nodes

--- #step Index authority subjects and objects
CREATE INDEX IF NOT EXISTS auth_subject_uuidx ON locid.auth_triples (subject_uuid);
CREATE INDEX IF NOT EXISTS auth_object_uuidx ON locid.auth_triples (object_uuid);

--- #step Analyze auth triples
--- #notx
VACUUM ANALYZE locid.auth_triples;

--- #step Index authority literal subjects
CREATE INDEX IF NOT EXISTS auth_lit_subject_uuidx ON locid.auth_literals (subject_uuid);

--- #step Analyze auth literals
--- #notx
VACUUM ANALYZE locid.auth_literals;

--- #step Extract authority node types
DROP MATERIALIZED VIEW IF EXISTS locid.auth_node_type;
CREATE MATERIALIZED VIEW locid.auth_node_type AS
SELECT DISTINCT subject_uuid, object_uuid
FROM locid.auth_triples
WHERE pred_uuid = node_uuid('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
CREATE INDEX auth_subj_idx ON locid.auth_node_type (subject_uuid);
CREATE INDEX auth_obj_idx ON locid.auth_node_type (object_uuid);

--- #step Analyze auth node types
--- #notx
VACUUM ANALYZE locid.auth_node_type;

--- #step Extract identifiable auth nodes
CREATE MATERIALIZED VIEW IF NOT EXISTS locid.auth_node_triples AS
SELECT sn.node_id AS subject_id, pn.node_id AS pred_id, object_uuid
FROM locid.auth_triples
JOIN locid.nodes sn ON (sn.node_uuid = subject_uuid)
JOIN locid.nodes pn ON (pn.node_uuid = pred_uuid);
CREATE INDEX IF NOT EXISTS auth_node_trip_subject_idx ON locid.auth_node_triples (subject_id);
CREATE INDEX IF NOT EXISTS auth_node_trip_object_idx ON locid.auth_node_triples (object_uuid);
ANALYZE locid.auth_node_triples;
