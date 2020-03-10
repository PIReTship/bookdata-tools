--- Schema for tables tracking the import process and related metadata

CREATE TABLE IF NOT EXISTS stage_status (
    stage_name VARCHAR PRIMARY KEY,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    stage_key VARCHAR NULL
);

CREATE TABLE IF NOT EXISTS source_file (
    filename VARCHAR NOT NULL PRIMARY KEY,
    reg_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    checksum VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS stage_file (
    stage_name VARCHAR NOT NULL REFERENCES stage_status,
    filename VARCHAR NOT NULL REFERENCES source_file,
    checksum VARCHAR NULL,
    PRIMARY KEY (stage_name, filename)
);

CREATE TABLE IF NOT EXISTS stage_dep (
    stage_name VARCHAR NOT NULL REFERENCES stage_status,
    dep_name VARCHAR NOT NULL REFERENCES stage_status,
    dep_key VARCHAR NULL
);

CREATE TABLE IF NOT EXISTS stage_table (
    stage_name VARCHAR NOT NULL REFERENCES stage_status,
    st_ns VARCHAR NOT NULL DEFAULT 'public',
    st_name VARCHAR NOT NULL
);

DO $cv$
BEGIN
    CREATE VIEW stage_table_oids
        AS SELECT stage_name, st_ns, st_name, c.oid AS oid, c.relkind AS kind
            FROM stage_table
            LEFT OUTER JOIN pg_namespace ns ON (ns.nspname = st_ns)
            LEFT OUTER JOIN pg_class c ON (c.relnamespace = ns.oid AND c.relname = st_name);
EXCEPTION
    WHEN duplicate_table THEN RETURN;
END;
$cv$;

INSERT INTO stage_status (stage_name, started_at, finished_at, stage_key)
VALUES ('init', NOW(), NOW(), uuid_generate_v4())
ON CONFLICT (stage_name) DO NOTHING;
