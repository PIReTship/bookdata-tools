--- #dep common-schema
--- #dep loc-id-schema

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
