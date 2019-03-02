--- #step Create auth labels
CREATE TABLE IF NOT EXISTS locid.auth_node_label (
  subject_id UUID NOT NULL,
  label VARCHAR NOT NULL
);

INSERT INTO locid.auth_node_label
SELECT DISTINCT subject_id, lit_value
FROM locid.auth_triple
JOIN locid.node_aliases pa ON (pred_id = pa.node_id)
JOIN locid.literals ON (object_id = lit_id)
WHERE node_alias IN ('label', 'auth-label');

CREATE INDEX IF NOT EXISTS auth_node_label_subj_idx
ON locid.auth_node_label (subject_id);
CREATE INDEX IF NOT EXISTS auth_node_label_lbl_idx
ON locid.auth_node_label (label);
ANALYZE locid.auth_node_label;

--- #step Summarize gender data
CREATE MATERIALIZED VIEW locid.mads_gender_summary AS
SELECT COUNT(t.subject_id), o.node_id, o.node_iri, l.label
FROM locid.auth_triple t
JOIN locid.node_aliases pa ON (pred_id = pa.node_id)
JOIN locid.nodes o ON (object_id = o.node_id)
LEFT JOIN locid.auth_node_label l ON (object_id = l.subject_id)
WHERE pa.node_alias = 'gender'
GROUP BY o.node_id, o.node_iri, l.label
ORDER BY COUNT(t.subject_id) DESC;

CREATE MATERIALIZED VIEW locid.skos_gender AS
SELECT nt.object_id AS node_id, l.label
FROM
  -- author triple
  locid.auth_triple nt
  -- author object to exclude
  LEFT JOIN locid.nodes ton ON (nt.object_id = ton.node_id)
  --- node type triples
  JOIN locid.auth_triple tt ON (nt.object_id = tt.subject_id)
  --- node labels
  JOIN locid.auth_node_label l ON (l.subject_id = nt.object_id)
WHERE
  nt.pred_id = locid.common_node('gender')
  AND ton.node_id IS NULL
  AND tt.pred_id = locid.common_node('type')
  AND tt.object_id = locid.common_node('concept');