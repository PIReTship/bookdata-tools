"""
Prepare GoodReads book clusters.
"""

from docopt import docopt

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from bookdata import script_log

_log = script_log('gr-book-clusters')

_log.info('reading book IDs')
books = pd.read_parquet('gr-book-ids.parquet', columns=['book_id', 'work_id'], use_nullable_dtypes=True)
books['book_id'] = books['book_id'].astype('uint32')
books.info()
books = books.set_index('book_id')
_log.info('reading ISBN IDs')
isbns = pd.read_parquet('book-isbn-ids.parquet')
isbns.info()
_log.info('reading clusters')
clusters = pd.read_parquet('../book-links/isbn-clusters.parquet', use_nullable_dtypes=True)
clusters = clusters.set_index('isbn_id')

_log.info('merging ISBN info')
merged = isbns.join(books, on='book_id', how='right')
_log.info('merging cluster info')
merged = merged.join(clusters, on='isbn_id', how='left')

_log.info('deduplicating clusters')
ids = merged[['book_id', 'work_id', 'cluster']].drop_duplicates()
ids.info()

_log.info('saving book ID files')
tbl = pa.Table.from_pandas(ids, preserve_index=False)
schema = pa.schema([
    pa.field('book_id', pa.uint32(), False),
    pa.field('work_id', pa.uint32(), True),
    pa.field('cluster', pa.int32(), True)
])
tbl = tbl.cast(schema)
_log.info('table schema:\n%s', tbl)
pq.write_table(tbl, 'gr-book-link.parquet', compression='zstd')
