--- #step Create auth labels
CREATE MATERIALIZED VIEW IF NOT EXISTS locid.auth_node_label AS
SELECT subject_id, lit_value AS label
FROM locid.auth_triple
JOIN locid.nodes pn ON (pred_id = node_id)
JOIN locid.literals ON (object_id = lit_id)
WHERE node_iri = 'http://www.w3.org/2000/01/rdf-schema#label';

CREATE INDEX IF NOT EXISTS auth_node_label_subj_idx
ON locid.auth_node_label (subject_id);
CREATE INDEX IF NOT EXISTS auth_node_label_lbl_idx
ON locid.auth_node_label (label);
ANALYZE locid.auth_node_label;
