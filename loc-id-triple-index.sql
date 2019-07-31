--- #step Index node IRIs
CREATE INDEX IF NOT EXISTS node_iri_idx ON locid.nodes (node_iri);

--- #step Vacuum and analyze node table
--- #notx
VACUUM ANALYZE locid.nodes;

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

--- #step Index well-known nodes
CREATE TABLE IF NOT EXISTS locid.node_aliases (
  node_alias VARCHAR UNIQUE NOT NULL,
  node_id INTEGER UNIQUE NOT NULL,
  node_uuid UUID UNIQUE NOT NULL,
  node_iri VARCHAR UNIQUE NOT NULL
);

CREATE OR REPLACE PROCEDURE locid.alias_node (alias VARCHAR, iri VARCHAR)
LANGUAGE plpgsql
AS $ln$
BEGIN
    INSERT INTO locid.node_aliases (node_alias, node_id, node_uuid, node_iri)
    SELECT alias, node_id, node_uuid, node_iri
    FROM locid.nodes
    WHERE node_iri = iri
    ON CONFLICT DO NOTHING;
END;
$ln$;

CREATE OR REPLACE FUNCTION locid.common_node(alias VARCHAR) RETURNS UUID
  LANGUAGE SQL STABLE PARALLEL SAFE COST 10
  AS $$
  SELECT node_uuid FROM locid.node_aliases WHERE node_alias = alias;
  $$;

CREATE OR REPLACE FUNCTION locid.common_nodeid(alias VARCHAR) RETURNS INTEGER
  LANGUAGE SQL STABLE PARALLEL SAFE COST 10
  AS $$
  SELECT node_id FROM locid.node_aliases WHERE node_alias = alias;
  $$;

CALL locid.alias_node('instance-of', 'http://id.loc.gov/ontologies/bibframe/instanceOf');
CALL locid.alias_node('label', 'http://www.w3.org/2000/01/rdf-schema#label');
CALL locid.alias_node('auth-label', 'http://www.loc.gov/mads/rdf/v1#authoritativeLabel');
CALL locid.alias_node('gender', 'http://www.loc.gov/mads/rdf/v1#gender');
CALL locid.alias_node('concept', 'http://www.w3.org/2004/02/skos/core#Concept');
CALL locid.alias_node('type', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
CALL locid.alias_node('isbn', 'http://id.loc.gov/ontologies/bibframe/Isbn');
CALL locid.alias_node('value', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#value');
CALL locid.alias_node('bf-id-by', 'http://id.loc.gov/ontologies/bibframe/identifiedBy');
CALL locid.alias_node('work', 'http://id.loc.gov/ontologies/bibframe/Work');
CALL locid.alias_node('instance', 'http://id.loc.gov/ontologies/bibframe/Instance');
ANALYSE locid.node_aliases;

--- #step Extract identifiable instance nodes
CREATE MATERIALIZED VIEW IF NOT EXISTS locid.instance_node_triples AS
SELECT sn.node_id AS subject_id, pn.node_id AS pred_id, object_uuid
FROM locid.instance_triples
JOIN locid.nodes sn ON (sn.node_uuid = subject_uuid)
JOIN locid.nodes pn ON (pn.node_uuid = pred_uuid);
CREATE INDEX IF NOT EXISTS instance_node_trip_subject_idx ON locid.instance_node_triples (subject_id);
CREATE INDEX IF NOT EXISTS instance_node_trip_object_idx ON locid.instance_node_triples (object_uuid);
ANALYZE locid.instance_node_triples;

--- #step Extract identifiable work nodes
CREATE MATERIALIZED VIEW IF NOT EXISTS locid.work_node_triples AS
SELECT sn.node_id AS subject_id, pn.node_id AS pred_id, object_uuid
FROM locid.work_triples
JOIN locid.nodes sn ON (sn.node_uuid = subject_uuid)
JOIN locid.nodes pn ON (pn.node_uuid = pred_uuid);
CREATE INDEX IF NOT EXISTS work_node_trip_subject_idx ON locid.work_node_triples (subject_id);
CREATE INDEX IF NOT EXISTS work_node_trip_object_idx ON locid.work_node_triples (object_uuid);
ANALYZE locid.work_node_triples;

--- #step Extract identifiable auth nodes
CREATE MATERIALIZED VIEW IF NOT EXISTS locid.auth_node_triples AS
SELECT sn.node_id AS subject_id, pn.node_id AS pred_id, object_uuid
FROM locid.auth_triples
JOIN locid.nodes sn ON (sn.node_uuid = subject_uuid)
JOIN locid.nodes pn ON (pn.node_uuid = pred_uuid);
CREATE INDEX IF NOT EXISTS auth_node_trip_subject_idx ON locid.auth_node_triples (subject_id);
CREATE INDEX IF NOT EXISTS auth_node_trip_object_idx ON locid.auth_node_triples (object_uuid);
ANALYZE locid.auth_node_triples;
