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
    return isbns[column].dropna().value_counts().sort_index()


def read_goodreads(file):
    books = pd.read_parquet(file, columns=['isbn10', 'isbn13', 'asin'])
    i10 = books['isbn10'].dropna().value_counts()
    i13 = books['isbn13'].dropna().value_counts()
    asin = books['asin'].dropna().value_counts()
    all = pd.concat([i10, i13, asin])
    if not all.index.is_unique:
        _log.warn('duplicate GR ISBNs')
        all = all.groupby(all.index).sum()
    return all.sort_index()

def read_amazon(file):
    asins = pd.read_csv(file, names=['user', 'asin', 'rating', 'timestamp'], usecols=['asin'])
    return asins['asin'].value_counts().sort_index()

_log.info('reading LOC ISBNs')
loc_isbns = read_column('loc-mds/book-isbns.parquet').to_frame(name='LOC')
_log.info('reading OL ISBNs')
ol_isbns = read_column('openlibrary/edition-isbns.parquet').to_frame('OL')
_log.info('reading GR ISBNs')
gr_isbns = read_goodreads('goodreads/gr-book-ids.parquet').to_frame('GR')
_log.info('reading BX ISBNs')
bx_isbns = read_column('bx/cleaned-ratings.csv', format='csv').to_frame('BX')
_log.info('reading Amazon ASINs')
az_asins = read_amazon('data/ratings_Books.csv').to_frame('AZ')

_log.info('combining ISBN lists')
isbns = loc_isbns.join(ol_isbns, how='outer')
isbns = isbns.join(gr_isbns, how='outer')
isbns = isbns.join(bx_isbns, how='outer')
isbns = isbns.join(az_asins, how='outer')
isbns.sort_index(inplace=True)
isbns = isbns.fillna(0).astype('i4')
_log.info('found %d unique ISBNs', len(isbns))
isbns['isbn_id'] = np.arange(len(isbns), dtype=np.int32) + 1
isbns.index.name = 'isbn'
assert isbns.index.is_unique
isbns = isbns.reset_index()

_log.info('writing output file')
isbns.to_parquet('book-links/all-isbns.parquet', compression='zstd', index=False)
