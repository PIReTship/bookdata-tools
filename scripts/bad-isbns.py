"""
Scan for bad ISBNs.

Usage:
    bad-isbns.py
"""

from bookdata import script_log
import pandas as pd

_log = script_log('bad-isbns')

_log.info('reading all-ISBN file')
isbn_frame = pd.read_parquet('book-links/all-isbns.parquet')
isbns = isbn_frame['isbn']

_log.info('finding ISBN matches')
# use a generous definition
is_isbn = isbns.str.match(r'^\d{7,15}[Xx]?$')
_log.info('finding ASIN matches')
is_asin = isbns.str.match(r'^A[A-Za-z0-9]+$')

valid = is_isbn | is_asin

bad = isbn_frame[~valid]

n_isbns = len(isbns)
n_bad = len(bad)

_log.info('%d / %d (%.2f%%) ISBNs look malformed',
          n_bad, n_isbns, n_bad / n_isbns * 100)
print(bad)
