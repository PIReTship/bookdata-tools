"""
Collect ISBNs from various sources.
"""

from docopt import docopt

import numpy as np
import pandas as pd

from bookdata import script_log

_log = script_log('collect-isbns')


def read_column(file, column='isbn', format='parquet'):
    if format == 'parquet':
        isbns = pd.read_parquet(file, columns=[column])
    elif format == 'csv':
        isbns = pd.read_csv(file)
    else:
        raise ValueError('invalid Parquet file')
    return np.unique(isbns[column].dropna())


def read_goodreads(file):
    books = pd.read_parquet(file, columns=['isbn', 'isbn13', 'asin'])
    isbns = np.concatenate([
        books['isbn'].dropna().unique(),
        books['isbn13'].dropna().unique(),
        books['asin'].dropna().unique()
    ])
    return isbns


_log.info('reading LOC ISBNs')
loc_isbns = read_column('loc-mds/book-isbns.parquet')
_log.info('reading OL ISBNs')
ol_isbns = read_column('openlibrary/edition-isbns.parquet')
_log.info('reading GR ISBNs')
gr_isbns = read_goodreads('goodreads/gr-book-ids.parquet')
_log.info('reading BX ISBNs')
bx_isbns = read_column('data/bx-ratings.csv', format='csv')

_log.info('combining ISBN lists')
all_isbns = np.unique(np.concatenate([
    loc_isbns, ol_isbns, gr_isbns, bx_isbns
]))
_log.info('found %d unique ISBNs', len(all_isbns))

_log.info('writing output file')
df = pd.DataFrame({
    'isbn_id': np.arange(len(all_isbns), dtype=np.int32),
    'isbn': all_isbns
})
df.to_parquet('book-links/all-isbns.parquet', compression='zstd', index=False)
