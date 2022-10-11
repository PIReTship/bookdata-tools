"""
Extract GoodReads work genders.

Usage:
    work-gender.py [options]

Options:
    -v, --verbose
        Turn on debug logging.
"""

from bookdata import script_log
from docopt import docopt

import polars as pl

opts = docopt(__doc__)
_log = script_log('work-gender', debug=opts['--verbose'])

_log.info('scanning input files')
gender = pl.scan_parquet('../book-links/cluster-genders.parquet')
books = pl.scan_parquet('gr-book-link.parquet')

merged = gender.join(books, on='cluster')
dedup = merged.unique(False)

_log.info('computing results')
results = dedup.collect()

_log.info('saving results')
results.write_parquet('gr-work-gender.parquet', compression='zstd')
