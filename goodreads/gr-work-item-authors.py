"""
Compute work-level "item" information for GoodReads.

Usage:
    gr-work-items.py
"""

import duckdb

GR_WORK_BASE = 400_000_000
GR_BOOK_BASE = 500_000_000


def main():
    with duckdb.connect() as db:
        db.execute("SET enable_progress_bar = true")
        db.read_parquet("gr-book-ids.parquet").create("book_ids")
        db.read_parquet("gr-book-authors.parquet").create("authors")

        db.execute(
            """
            COPY (
                WITH book_authors AS (
                    SELECT DISTINCT item_id, author_id
                    FROM book_ids
                    JOIN authors USING (book_id)
                )
                SELECT item_id, author_id, name as author_name
                FROM book_authors
                LEFT JOIN 'gr-author-info.parquet' USING (author_id)
            ) TO 'gr-work-item-authors.parquet' (COMPRESSION zstd)
            """
        )


if __name__ == "__main__":
    main()
