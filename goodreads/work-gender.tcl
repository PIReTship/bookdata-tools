table gender "../book-links/cluster-genders.parquet"
table book_links "gr-book-link.parquet"

save-results "gr-work-gender.parquet" {
    SELECT DISTINCT work_id, cluster, gender
    FROM book_links
    JOIN gender USING (cluster)
}
