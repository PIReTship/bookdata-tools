"""
Prepare GoodReads book clusters.
"""

from docopt import docopt

import numpy as np
import pandas as pd

from bookdata import script_log

_log = script_log('gr-book-clusters')

_log.info('reading book IDs')
books = pd.read_parquet('gr-book-ids.parquet', columns=['book_id', 'work_id'], use_nullable_dtypes=True)
# books['work_id'] = books['work_id'].astype('UInt32')
books.info()
books = books.set_index('book_id')
_log.info('reading ISBN IDs')
isbns = pd.read_parquet('book-isbn-ids.parquet')
isbns.info()
_log.info('reading clusters')
clusters = pd.read_parquet('../book-links/isbn-clusters.parquet')
clusters = clusters.set_index('isbn_id')

_log.info('merging ISBN info')
merged = isbns.join(books, on='book_id')
_log.info('merging cluster info')
merged = merged.join(clusters, on='isbn_id')

_log.info('deduplicating clusters')
ids = merged[['book_id', 'work_id', 'cluster']].drop_duplicates()

_log.info('saving book ID files')
ids.to_parquet('gr-book-link.parquet', index=False, compression='zstd')
