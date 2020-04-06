--- #dep cluster
--- #table gr.cluster_stats
--- #table locmds.cluster_stats
--- #table ol.cluster_stats
--- #table cluster_stats
--- #step Count GoodReads cluster statistics
DROP MATERIALIZED VIEW IF EXISTS gr.cluster_stats CASCADE;
CREATE MATERIALIZED VIEW gr.cluster_stats AS
SELECT cluster,
    COUNT(DISTINCT gr_book_id) AS gr_books,
    COUNT(DISTINCT gr_work_id) AS gr_works
FROM gr.book_cluster
JOIN gr.book_ids USING (gr_book_id)
GROUP BY cluster;
CREATE UNIQUE INDEX gr_cluster_stat_cluster_idx ON gr.cluster_stats (cluster);
ANALYZE gr.cluster_stats;

--- #step Count LOC-MDS cluster statistics
DROP MATERIALIZED VIEW IF EXISTS locmds.cluster_stats CASCADE;
CREATE MATERIALIZED VIEW locmds.cluster_stats AS
SELECT cluster, COUNT(DISTINCT rec_id) AS loc_recs
FROM isbn_cluster
JOIN locmds.book_rec_isbn USING (isbn_id)
GROUP BY cluster;
CREATE UNIQUE INDEX loc_cluster_stat_cluster_idx ON locmds.cluster_stats(cluster);
ANALYZE locmds.cluster_stats;

--- #step Count OpenLib cluster statistics
DROP MATERIALIZED VIEW IF EXISTS ol.cluster_stats CASCADE;
CREATE MATERIALIZED VIEW ol.cluster_stats AS
SELECT cluster,
    COUNT(DISTINCT edition_id) AS ol_editions,
    COUNT(DISTINCT work_id) AS ol_works
FROM isbn_cluster
JOIN ol.isbn_link USING (isbn_id)
GROUP BY cluster;
CREATE UNIQUE INDEX ol_cluster_stat_cluster_idx ON ol.cluster_stats(cluster);
ANALYZE ol.cluster_stats;

--- #step Create joing statistics table
DROP MATERIALIZED VIEW IF EXISTS cluster_stats CASCADE;
CREATE MATERIALIZED VIEW cluster_stats AS
WITH isbn_stats AS (SELECT cluster, COUNT(isbn_id) AS isbns
                    FROM isbn_cluster
                    GROUP BY cluster)
SELECT cluster, isbns, loc_recs, ol_editions, ol_works, gr_books, gr_works
FROM isbn_stats
LEFT JOIN locmds.cluster_stats USING (cluster)
LEFT JOIN gr.cluster_stats USING (cluster)
LEFT JOIN ol.cluster_stats USING (cluster);
CREATE UNIQUE INDEX cluster_stat_cluster_idx ON cluster_stats (cluster);
