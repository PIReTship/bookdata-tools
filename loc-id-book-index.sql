--- #step Extract instance IRIs
DROP MATERIALIZED VIEW IF EXISTS locid.instance_entity CASCADE;
CREATE MATERIALIZED VIEW locid.instance_entity AS
SELECT DISTINCT sn.node_id AS instance_id, subject_uuid AS instance_uuid,
  sn.node_iri AS instance_iri
FROM locid.instance_triples
JOIN locid.nodes sn ON (subject_uuid = sn.node_uuid)
WHERE pred_uuid = node_uuid('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
  AND object_uuid = node_uuid('http://id.loc.gov/ontologies/bibframe/Instance');
CREATE UNIQUE INDEX instance_inst_id_idx ON locid.instance_entity (instance_id);
CREATE UNIQUE INDEX instance_inst_uuid_idx ON locid.instance_entity (instance_uuid);

--- #step Analyze instance IRIs
--- #notx
VACUUM ANALYZE locid.instance_entity;

--- #step Extract work IRIs
DROP MATERIALIZED VIEW IF EXISTS locid.work_entity CASCADE;
CREATE MATERIALIZED VIEW locid.work_entity AS
SELECT sn.node_id AS work_id, subject_uuid AS work_uuid,
  sn.node_iri AS work_iri
FROM locid.work_triples
JOIN locid.nodes sn ON (subject_uuid = sn.node_uuid)
WHERE pred_uuid = node_uuid('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
  AND object_uuid = node_uuid('http://id.loc.gov/ontologies/bibframe/Work');
CREATE INDEX work_inst_id_idx ON locid.work_entity (work_id);
CREATE INDEX work_inst_uuid_idx ON locid.work_entity (work_uuid);

--- #step Analyze work IRIs
--- #notx
VACUUM ANALYZE locid.work_entity;

--- #step Extract instance/work relationships
DROP MATERIALIZED VIEW IF EXISTS locid.instance_work CASCADE;
CREATE MATERIALIZED VIEW locid.instance_work AS
SELECT DISTINCT isn.node_id as instance_id, isn.node_uuid as instance_uuid,
  wsn.node_id AS work_id, wsn.node_uuid AS work_uuid
FROM locid.instance_triples it
JOIN locid.nodes isn ON (isn.node_uuid = it.subject_uuid)
JOIN locid.nodes wsn ON (wsn.node_uuid = it.object_uuid)
JOIN locid.work_entity we ON (it.object_uuid = we.work_uuid);
CREATE INDEX instance_work_instance_idx ON locid.instance_work (instance_uuid);
CREATE INDEX instance_work_work_idx ON locid.instance_work (work_uuid);

--- #step Index instance ISBNs
DROP MATERIALIZED VIEW IF EXISTS locid.instance_ext_isbn CASCADE;
CREATE MATERIALIZED VIEW locid.instance_ext_isbn AS
SELECT tt.subject_uuid AS subject_uuid,
  il.lit_value AS raw_isbn,
  extract_isbn(il.lit_value) AS isbn
FROM locid.instance_triples tt
JOIN locid.instance_literals il USING (subject_uuid)
WHERE
  -- subject is of type ISBN
  tt.pred_uuid = locid.common_node('type')
  AND tt.object_uuid = locid.common_node('isbn')
  -- we have a literal value
  AND il.pred_uuid = locid.common_node('value');
CREATE INDEX instance_ext_isbn_node_idx ON locid.instance_ext_isbn (subject_uuid);
ANALYZE locid.instance_ext_isbn;

--- #step Index work ISBNs
DROP MATERIALIZED VIEW IF EXISTS locid.work_ext_isbn CASCADE;
CREATE MATERIALIZED VIEW locid.work_ext_isbn AS
SELECT tt.subject_uuid AS subject_uuid,
  wl.lit_value AS raw_isbn,
  extract_isbn(wl.lit_value) AS isbn
FROM locid.work_triples tt
JOIN locid.work_literals wl USING (subject_uuid)
WHERE
  -- subject is of type ISBN
  tt.pred_uuid = locid.common_node('type')
  AND tt.object_uuid = locid.common_node('isbn')
  -- we have a literal value
  AND wl.pred_uuid = locid.common_node('value');
CREATE INDEX work_ext_isbn_node_idx ON locid.work_ext_isbn (subject_uuid);
ANALYZE locid.work_ext_isbn;


--- #step Enroll ISBNs
INSERT INTO isbn_id (isbn)
SELECT DISTINCT isbn
FROM locid.instance_ext_isbn
WHERE char_length(isbn) IN (10,13)
  AND isbn NOT IN (SELECT isbn FROM isbn_id);

INSERT INTO isbn_id (isbn)
SELECT DISTINCT isbn
FROM locid.work_ext_isbn
WHERE char_length(isbn) IN (10,13)
  AND isbn NOT IN (SELECT isbn FROM isbn_id);

DROP MATERIALIZED VIEW IF EXISTS locid.instance_isbn CASCADE;
CREATE MATERIALIZED VIEW locid.instance_isbn AS
SELECT instance_id, instance_uuid, isbn_id
FROM locid.instance_ext_isbn xi
JOIN locid.instance_triples it ON (xi.subject_uuid = it.object_uuid)
JOIN locid.instance_entity ON (it.subject_uuid = instance_uuid)
JOIN isbn_id USING (isbn)
WHERE it.pred_uuid = locid.common_node('bf-id-by');
CREATE INDEX instance_isbn_idx ON locid.instance_isbn (instance_id);
CREATE INDEX instance_isbn_node_idx ON locid.instance_isbn (instance_uuid);
CREATE INDEX instance_isbn_isbn_idx ON locid.instance_isbn (isbn_id);
ANALYZE locid.instance_isbn;

DROP MATERIALIZED VIEW IF EXISTS locid.work_isbn CASCADE;
CREATE MATERIALIZED VIEW locid.work_isbn AS
SELECT work_id, work_uuid, isbn_id
FROM (locid.work_ext_isbn xi
JOIN locid.work_triples wt ON (xi.subject_uuid = wt.object_uuid))
JOIN locid.work_entity ON (wt.subject_uuid = work_uuid)
JOIN isbn_id USING (isbn)
WHERE wt.pred_uuid = locid.common_node('bf-id-by');
CREATE INDEX work_isbn_idx ON locid.work_isbn (work_id);
CREATE INDEX work_isbn_node_idx ON locid.work_isbn (work_uuid);
CREATE INDEX work_isbn_isbn_idx ON locid.work_isbn (isbn_id);
ANALYZE locid.work_isbn;

--- #step Set up ISBN Link table
DROP TABLE IF EXISTS locid.isbn_link CASCADE;
CREATE TABLE locid.isbn_link (
  isbn_id INTEGER NOT NULL,
  instance_id INTEGER,
  work_id INTEGER,
  book_code INTEGER
);

INSERT INTO locid.isbn_link (isbn_id, work_id, book_code)
SELECT isbn_id, work_id, bc_of_loc_work(work_id)
FROM locid.work_isbn;

INSERT INTO locid.isbn_link (isbn_id, instance_id, work_id, book_code)
SELECT isbn_id, instance_id, work_id, COALESCE(bc_of_loc_work(work_id), bc_of_loc_instance(instance_id))
FROM locid.instance_isbn
LEFT JOIN locid.instance_work USING (instance_id);

CREATE INDEX isbn_link_isbn_idx ON locid.isbn_link (isbn_id);
CREATE INDEX isbn_link_work_idx ON locid.isbn_link (work_id);
CREATE INDEX isbn_link_instance_idx ON locid.isbn_link (work_id);
CREATE INDEX isbn_link_bc_idx ON locid.isbn_link (book_code);
