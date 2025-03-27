"""
Compute work-level "item" information for GoodReads.

Usage:
    gr-work-items.py [--first]
"""

import duckdb
from docopt import docopt

GR_WORK_BASE = 400_000_000
GR_BOOK_BASE = 500_000_000


def main():
    opts = docopt(__doc__ or "")

    with duckdb.connect() as db:
        db.execute("SET enable_progress_bar = true")
        db.read_parquet("gr-book-ids.parquet").create("book_ids")
        db.read_parquet("gr-book-authors.parquet").create("authors")

        author_tbl = "authors"
        fn = "gr-work-item-authors"
        if opts["--first"]:
            db.execute(
                """
                CREATE VIEW first_authors AS (
                    SELECT book_id, any_value(author_id ORDER BY position) AS author_id
                    FROM authors
                    WHERE role IS NULL
                    GROUP BY book_id
                )
                """
            )
            author_tbl = "first_authors"
            fn = "gr-work-item-first-authors"

        db.execute(
            f"""
            COPY (
                WITH book_authors AS (
                    SELECT DISTINCT item_id, author_id
                    FROM book_ids
                    JOIN {author_tbl} USING (book_id)
                )
                SELECT item_id, author_id, name as author_name
                FROM book_authors
                LEFT JOIN 'gr-author-info.parquet' USING (author_id)
            ) TO '{fn}.parquet' (COMPRESSION zstd)
            """
        )


if __name__ == "__main__":
    main()
