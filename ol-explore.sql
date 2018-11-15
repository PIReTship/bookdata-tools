--- Views and such for understanding the contents of the OpenLibrary data
CREATE MATERIALIZED VIEW ol_edition_json_keys
     AS SELECT json_key, COUNT(edition_id)
        FROM (SELECT edition_id, jsonb_object_keys(edition_data) AS json_key
              FROM ol_edition) eks
        GROUP BY json_key;


CREATE MATERIALIZED VIEW ol_work_json_keys
     AS SELECT json_key, COUNT(work_id)
        FROM (SELECT work_id, jsonb_object_keys(work_data) AS json_key
              FROM ol_work) eks
        GROUP BY json_key;