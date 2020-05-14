--- #dep ol-index
-- Extract book information from OpenLibrary

--- #step Extract edition titles
CREATE MATERIALIZED VIEW IF NOT EXISTS ol.edition_title
AS SELECT edition_id, edition_data->>'title' AS title
    FROM ol.edition;
CREATE INDEX IF NOT EXISTS ol_edition_title_idx ON ol.edition_title (edition_id);
ANALYZE ol.edition_title;

--- #step Extract work titles
CREATE MATERIALIZED VIEW IF NOT EXISTS ol.work_title
AS SELECT work_id, work_data->>'title' AS title
    FROM ol.work;
CREATE INDEX IF NOT EXISTS ol_work_title_idx ON ol.work_title (work_id);
ANALYZE ol.work_title;
