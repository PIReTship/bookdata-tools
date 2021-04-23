--- #dep loc-id-triple-names
--- #table locid.gender_nodes

--- #step Create gender table
DROP MATERIALIZED VIEW IF EXISTS locid.gender_nodes CASCADE;
CREATE MATERIALIZED VIEW locid.gender_nodes
AS SELECT DISTINCT subject_uuid AS node_uuid, su.node_iri, label
    FROM locid.auth_triples
    JOIN locid.nodes su ON (subject_uuid = su.node_uuid)
    JOIN locid.nodes pr ON (pred_uuid = pr.node_uuid)
    JOIN locid.nodes ob ON (object_uuid = ob.node_uuid)
    LEFT JOIN locid.auth_node_label USING (subject_uuid)
    WHERE (pr.node_iri = 'http://www.loc.gov/mads/rdf/v1#isMemberOfMADSCollection'
           AND ob.node_iri = 'http://id.loc.gov/authorities/demographicTerms/collection_LCDGT_Gender')
       OR (pr.node_iri = 'http://www.w3.org/2004/02/skos/core#inScheme'
           AND ob.node_iri = 'http://id.loc.gov/authorities/demographicTerms/gdr');
