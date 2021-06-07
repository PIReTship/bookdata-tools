"""
Link together GoodReads book clusters.
"""

import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from bookdata import script_log

_log = script_log("book-clusters")

_log.info('reading input tables')
books = pd.read_parquet("gr-book-ids.parquet", columns=["book_id", "work_id"])
book_isbns = pd.read_parquet("book-isbn-ids.parquet")
clusters = pd.read_parquet("../book-links/isbn-clusters.parquet", columns=["isbn_id", "cluster"])

_log.info('merging tables')
merged = books.join(book_isbns.set_index("book_id"), on="book_id", how="left")
merged = merged.join(clusters.set_index("isbn_id"), on="isbn_id", how="left")

_log.info('de-duplicating')
merged = pd.DataFrame({
    'book_id': merged['book_id'],
    'work_id': merged['work_id'].astype('Int64'),
    'cluster': merged['cluster'].astype('Int64'),
})
merged = merged.drop_duplicates()

_log.info('writing')
table = pa.Table.from_pandas(merged, preserve_index=False)
_log.info('raw table:\n%s', table)
schema = pa.schema([
    pa.field('book_id', pa.int32(), False),
    pa.field('work_id', pa.int32(), True),
    pa.field('cluster', pa.int32(), True)
])
table = table.cast(schema)
_log.info('converted table:\n%s', table)
pq.write_table(table, 'gr-book-link.parquet', compression='zstd')
