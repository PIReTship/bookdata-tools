"""
Compute non-deduplicated actions for GoodReads works.

Usage:
    gr-work-all-actions.py
"""

import polars as pl

GR_WORK_BASE = 400_000_000
GR_BOOK_BASE = 500_000_000


def main():
    ids = pl.scan_parquet("gr-book-ids.parquet")
    actions = pl.scan_parquet("gr-interactions.parquet")
    actions = actions.join(ids, "book_id")

    actions = actions.select(
        "user_id",
        "item_id",
        "book_id",
        "work_id",
        "rating",
        "is_read",
        pl.col("added").alias("timestamp"),
        pl.col("updated"),
    )

    print("saving actions")
    actions.sink_parquet("gr-work-all-actions.parquet")


if __name__ == "__main__":
    main()
