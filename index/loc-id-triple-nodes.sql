--- #dep loc-id-names

--- #step Index node IRIs
CREATE INDEX IF NOT EXISTS node_iri_idx ON locid.nodes (node_iri);

--- #step Vacuum and analyze node table
--- #notx
VACUUM ANALYZE locid.nodes;
