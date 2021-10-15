table editions "openlibrary/editions.parquet"
table works "openlibrary/works.parquet"
table edition_works "openlibrary/edition-works.parquet"

set id 18334306

query {
    SELECT *
    FROM works
    WHERE key = '/works/OL8193418W'
}

query {
    SELECT *
    FROM editions
    WHERE key = '/books/OL29216808M'
}

query {
    SELECT *
    FROM edition_works
    WHERE work = 18334306
}
