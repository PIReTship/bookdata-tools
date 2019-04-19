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
