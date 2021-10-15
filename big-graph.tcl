table editions "openlibrary/editions.parquet"
table works "openlibrary/works.parquet"
table edition_works "openlibrary/edition-works.parquet"
table ed_isbn "openlibrary/edition-isbns.parquet"

set id 9312043

query {
    SELECT *
    FROM works
    WHERE id = 9312043
}

query {
    SELECT COUNT(*)
    FROM edition_works
    WHERE work = 9312043
}

query {
    SELECT isbn, COUNT(ew.edition)
    FROM edition_works ew
    JOIN ed_isbn ei ON ew.edition = ei.edition
    WHERE ew.work = 9312043
    GROUP BY isbn
    HAVING COUNT(ew.edition) > 1
}
