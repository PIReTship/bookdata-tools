"""
Compute work-level "item" information for GoodReads.

Usage:
    gr-work-items.py
"""

import polars as pl

GR_WORK_BASE = 400_000_000
GR_BOOK_BASE = 500_000_000


def main():
    ids = pl.scan_parquet("gr-book-ids.parquet")
    bgs = pl.scan_parquet("gr-book-genres.parquet")

    bgs = ids.join(bgs, "book_id", how="inner")
    item_genres = bgs.group_by("item_id", "genre_id").agg(pl.col("count").sum().alias("count"))

    item_genres = item_genres.collect()
    item_genres.write_parquet("gr-work-item-genres.parquet", compression="zstd")


if __name__ == "__main__":
    main()
