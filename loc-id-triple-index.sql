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

--- #step Index well-known nodes
CREATE TABLE IF NOT EXISTS locid.node_aliases (
  node_alias VARCHAR UNIQUE NOT NULL,
  node_id UUID UNIQUE NOT NULL,
  node_iri VARCHAR UNIQUE NOT NULL
);

CREATE OR REPLACE PROCEDURE locid.alias_node (alias VARCHAR, iri VARCHAR)
LANGUAGE plpgsql
AS $ln$
BEGIN
    INSERT INTO locid.node_aliases (node_alias, node_id, node_iri)
    SELECT alias, node_id, node_iri
    FROM locid.nodes
    WHERE node_iri = iri
    ON CONFLICT DO NOTHING;
END;
$ln$;

CREATE OR REPLACE FUNCTION locid.common_node(alias VARCHAR) RETURNS UUID
  LANGUAGE SQL STABLE
  AS $$
  SELECT node_id FROM locid.node_aliases WHERE node_alias = alias;
  $$;

CALL locid.alias_node('label', 'http://www.w3.org/2000/01/rdf-schema#label');
CALL locid.alias_node('auth-label', 'http://www.loc.gov/mads/rdf/v1#authoritativeLabel');
CALL locid.alias_node('gender', 'http://www.loc.gov/mads/rdf/v1#gender');
CALL locid.alias_node('concept', 'http://www.w3.org/2004/02/skos/core#Concept');
CALL locid.alias_node('type', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
ANALYSE locid.node_aliases;

