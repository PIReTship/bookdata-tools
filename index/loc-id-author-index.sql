--- #dep loc-id-triple-names

--- #step Index authority / RWO links
DROP MATERIALIZED VIEW IF EXISTS locid.auth_name_rwo CASCADE;
CREATE MATERIALIZED VIEW locid.auth_name_rwo AS
SELECT subject_uuid AS auth_uuid, object_uuid AS rwo_uuid
FROM locid.auth_triples
WHERE pred_uuid = node_uuid('http://www.loc.gov/mads/rdf/v1#identifiesRWO');
CREATE INDEX auth_rwo_auth_idx ON locid.auth_name_rwo (auth_uuid);
CREATE INDEX auth_rwo_rwo_idx ON locid.auth_name_rwo (rwo_uuid);
ANALYZE locid.auth_name_rwo;

--- #step Index authority entities
DROP MATERIALIZED VIEW IF EXISTS locid.auth_entity CASCADE;
CREATE MATERIALIZED VIEW locid.auth_entity AS
SELECT ant.subject_id AS auth_id, sn.node_uuid AS auth_uuid, sn.node_iri AS auth_iri
FROM locid.auth_node_triples ant
JOIN locid.nodes sn ON (ant.subject_id = sn.node_id)
JOIN locid.nodes pn ON (ant.pred_id = pn.node_id)
WHERE pn.node_iri = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
  AND ant.object_uuid = node_uuid('http://www.loc.gov/mads/rdf/v1#Authority');
CREATE INDEX auth_entity_idx ON locid.auth_entity (auth_id);
CREATE INDEX auth_entity_uuidx ON locid.auth_entity (auth_uuid);
CREATE INDEX auth_entity_iri_idx ON locid.auth_entity (auth_iri);
ANALYZE locid.auth_entity;

-- --- #step Extract contributions from works
-- --- #allow duplicate_object
-- -- This view will map internal contribution node UUIDs to object UUIDs
-- -- referencing the 'agent' (contributor).
-- CREATE MATERIALIZED VIEW locid.work_contribution AS
-- SELECT DISTINCT wt.subject_uuid, wt.object_uuid
-- FROM locid.work_triples wt
--   -- we need to check the role
--   JOIN locid.work_triples rt USING (subject_uuid)
--   -- and the type - we want primary contributions
--   JOIN locid.work_node_type tt USING (subject_uuid)
-- WHERE wt.pred_uuid = node_uuid('http://id.loc.gov/ontologies/bibframe/agent')
--   AND rt.pred_uuid = node_uuid('http://id.loc.gov/ontologies/bibframe/role')
--   AND rt.object_uuid = node_uuid('http://id.loc.gov/vocabulary/relators/ctb')
--   AND tt.object_uuid = node_uuid('http://id.loc.gov/ontologies/bflc/PrimaryContribution');
-- CREATE INDEX wc_subj_idx ON locid.work_contribution (subject_uuid);
-- CREATE INDEX wc_obj_idx ON locid.work_contribution (object_uuid);
-- ANALYZE locid.work_triples;

--- #step Create auth labels
CREATE TABLE IF NOT EXISTS locid.auth_node_label (
  subject_uuid UUID NOT NULL,
  label VARCHAR NOT NULL
);

INSERT INTO locid.auth_node_label
SELECT DISTINCT subject_uuid, lit_value
FROM locid.auth_literals
WHERE pred_uuid = node_uuid('http://www.w3.org/2000/01/rdf-schema#label')
   OR pred_uuid = node_uuid('http://www.loc.gov/mads/rdf/v1#authoritativeLabel');

CREATE INDEX IF NOT EXISTS auth_node_label_subj_idx
ON locid.auth_node_label (subject_uuid);
CREATE INDEX IF NOT EXISTS auth_node_label_lbl_idx
ON locid.auth_node_label (label);
ANALYZE locid.auth_node_label;

--- #step Count node occurrences in subject & object positions
DROP MATERIALIZED VIEW IF EXISTS locid.auth_node_count_subject CASCADE;
CREATE MATERIALIZED VIEW locid.auth_node_count_subject AS
SELECT node_id, node_uuid, COUNT(object_uuid) AS n_sub_triples
FROM locid.nodes
JOIN locid.auth_triples ON (subject_uuid = node_uuid)
GROUP BY node_id;

CREATE INDEX auth_node_count_subject_node_idx ON locid.auth_node_count_subject (node_id);
CREATE INDEX auth_node_count_subject_node_uuidx ON locid.auth_node_count_subject (node_uuid);
ANALYZE locid.auth_node_count_subject;

DROP MATERIALIZED VIEW IF EXISTS locid.auth_node_count_object CASCADE;
CREATE MATERIALIZED VIEW locid.auth_node_count_object AS
SELECT node_id, node_uuid, COUNT(subject_uuid) AS n_obj_triples
FROM locid.nodes
JOIN locid.auth_triples ON (object_uuid = node_uuid)
GROUP BY node_id;

CREATE INDEX auth_node_count_object_node_idx ON locid.auth_node_count_object (node_id);
CREATE INDEX auth_node_count_object_node_uuidx ON locid.auth_node_count_object (node_uuid);
ANALYZE locid.auth_node_count_object;

--- #step Get entity genders
DROP MATERIALIZED VIEW IF EXISTS locid.auth_gender;
CREATE MATERIALIZED VIEW locid.auth_gender
AS SELECT auth_id, auth_uuid, rwo_uuid, gn.node_uuid AS gender_uuid
FROM locid.auth_entity
JOIN locid.auth_name_rwo USING (auth_uuid)
JOIN locid.auth_triples ON (rwo_uuid = subject_uuid)
JOIN locid.gender_nodes gn ON (object_uuid = gn.node_uuid)
WHERE pred_uuid = node_uuid('http://www.loc.gov/mads/rdf/v1#gender');
