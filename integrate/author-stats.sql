--- #dep author-info
--- #step Set up statistics table
DROP TABLE IF EXISTS integration_stats CASCADE;
CREATE TABLE integration_stats (
    dataset VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    n_books INTEGER,
    n_actions INTEGER
);

--- #step Count LOC integration statistics
WITH
    books AS (SELECT DISTINCT cluster
              FROM locmds.book_rec_isbn JOIN isbn_cluster USING (isbn_id))
INSERT INTO integration_stats (dataset, gender, n_books)
SELECT 'LOC-MDS', gender, COUNT(cluster)
FROM books JOIN cluster_first_author_gender USING (cluster)
GROUP BY gender;

--- #step Count BookCrossing integration statistics
INSERT INTO integration_stats (dataset, gender, n_books, n_actions)
SELECT 'BX-I', COALESCE(gender, 'no-book'), COUNT(DISTINCT book_id), COUNT(book_id)
FROM bx.add_action
LEFT JOIN cluster_first_author_gender ON (book_id = cluster)
GROUP BY COALESCE(gender, 'no-book');

INSERT INTO integration_stats (dataset, gender, n_books, n_actions)
SELECT 'BX-E', COALESCE(gender, 'no-book'), COUNT(DISTINCT book_id), COUNT(book_id)
FROM bx.rating
LEFT JOIN cluster_first_author_gender ON (book_id = cluster)
GROUP BY COALESCE(gender, 'no-book');

--- #step Count Amazon 14 integration statistics
INSERT INTO integration_stats (dataset, gender, n_books, n_actions)
SELECT 'AZ', COALESCE(gender, 'no-book'), COUNT(DISTINCT book_id), COUNT(book_id)
FROM az14.rating
LEFT JOIN cluster_first_author_gender ON (book_id = cluster)
GROUP BY COALESCE(gender, 'no-book');

--- #step Count Amazon 18 integration statistics
INSERT INTO integration_stats (dataset, gender, n_books, n_actions)
SELECT 'AZ18', COALESCE(gender, 'no-book'), COUNT(DISTINCT book_id), COUNT(book_id)
FROM az18.rating
LEFT JOIN cluster_first_author_gender ON (book_id = cluster)
GROUP BY COALESCE(gender, 'no-book');

--- #step Count GoodReads integration statistics
INSERT INTO integration_stats (dataset, gender, n_books, n_actions)
SELECT 'GR-I', COALESCE(gender, 'no-book'), COUNT(DISTINCT book_id), COUNT(book_id)
FROM gr.add_action
LEFT JOIN cluster_first_author_gender ON (book_id = cluster)
GROUP BY COALESCE(gender, 'no-book');

INSERT INTO integration_stats (dataset, gender, n_books, n_actions)
SELECT 'GR-E', COALESCE(gender, 'no-book'), COUNT(DISTINCT book_id), COUNT(book_id)
FROM gr.rating
LEFT JOIN cluster_first_author_gender ON (book_id = cluster)
GROUP BY COALESCE(gender, 'no-book');
