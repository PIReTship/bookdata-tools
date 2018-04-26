DROP TABLE IF EXISTS loc_isbn_cluster CASCADE;
CREATE TABLE loc_isbn_cluster (
    isbn_id INTEGER NOT NULL,
    cluster INTEGER NOT NULL
);
\copy loc_isbn_cluster FROM 'data/loc-clusters.csv' WITH (FORMAT CSV);
ALTER TABLE loc_isbn_cluster ADD PRIMARY KEY (isbn_id);
CREATE INDEX loc_isbn_cluster_idx ON loc_isbn_cluster (cluster);
ANALYZE loc_isbn_cluster;

DROP TABLE IF EXISTS ol_isbn_cluster CASCADE;
CREATE TABLE ol_isbn_cluster (
    isbn_id INTEGER NOT NULL,
    cluster INTEGER NOT NULL
);
\copy ol_isbn_cluster FROM 'data/ol-clusters.csv' WITH (FORMAT CSV);
ALTER TABLE ol_isbn_cluster ADD PRIMARY KEY (isbn_id);
CREATE INDEX ol_isbn_cluster_idx ON ol_isbn_cluster (cluster);
ANALYZE ol_isbn_cluster;

DROP TABLE IF EXISTS isbn_cluster CASCADE;
CREATE TABLE isbn_cluster (
    isbn_id INTEGER NOT NULL,
    cluster INTEGER NOT NULL
);
\copy isbn_cluster FROM 'data/isbn-clusters.csv' WITH (FORMAT CSV);
ALTER TABLE isbn_cluster ADD PRIMARY KEY (isbn_id);
CREATE INDEX isbn_cluster_idx ON isbn_cluster (cluster);
ANALYZE isbn_cluster;