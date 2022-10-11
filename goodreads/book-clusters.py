"""
Align GoodReads books with clusters.

Usage:
    book-clusters.py [options]

Options:
    -v, --verbose
        Turn on debug logging.
"""

from bookdata import script_log
from bookdata.schema import MULT_BASE, ns_gr_book
from docopt import docopt

import polars as pl

opts = docopt(__doc__)
_log = script_log('book-clusters', debug=opts['--verbose'])

_log.info('setting up inputs')
clusters = pl.scan_parquet('../book-links/cluster-graph-nodes.parquet')
books = pl.scan_parquet('gr-book-ids.parquet')

clusters = clusters.filter(
    pl.col('book_code') // pl.lit(MULT_BASE) == pl.lit(ns_gr_book.code)
).select([
    (pl.col('book_code') - pl.lit(ns_gr_book.offset)).alias('book_id'),
    pl.col('cluster')
])

links = books.join(clusters, on='book_id').select([
    'book_id', 'work_id', 'cluster'
])

_log.info('collecting results')
links = links.collect()

_log.info('writing %d records to output', links.height)
links.write_parquet('gr-book-link.parquet', compression='zstd')

_log.info('finished')
