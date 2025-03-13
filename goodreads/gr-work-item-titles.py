"""
Compute work-level "item" information for GoodReads.

Usage:
    gr-work-item-titles.py
"""

import polars as pl

GR_WORK_BASE = 400_000_000
GR_BOOK_BASE = 500_000_000


def main():
    ids = pl.scan_parquet("gr-book-ids.parquet")
    works = (
        ids.filter(pl.col("work_id").is_not_null())
        .select(
            pl.col("item_id"),
            pl.col("work_id"),
            pl.lit(None).alias("book_id"),
        )
        .unique()
    )
    books = (
        ids.filter(pl.col("work_id").is_null())
        .select(
            pl.col("item_id"),
            pl.col("work_id"),
            pl.col("book_id"),
        )
        .unique()
    )

    book_info = pl.scan_parquet("gr-book-info.parquet").select("book_id", "title")
    work_info = pl.scan_parquet("gr-work-info.parquet").select("work_id", "title")

    books = books.join(book_info, "book_id", how="left")
    works = works.join(work_info, "work_id", how="left")
    items = pl.concat([books, works])

    items = items.collect()
    items.write_parquet("gr-work-item-titles.parquet", compression="zstd")


if __name__ == "__main__":
    main()
