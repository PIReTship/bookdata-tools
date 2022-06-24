"""
Extract BookCrossing actions for book clusters.

Usage:
    bx-cluster-actions.py [options]

Options:
    -v, --verbose
        Turn on debug logging.
"""

from bookdata import script_log
from docopt import docopt

import polars as pl

opts = docopt(__doc__)
_log = script_log('bx-cluster-actions', debug=opts['--verbose'])

_log.info('setting up data inputs')
isbns = pl.scan_parquet("../book-links/isbn-clusters.parquet")
isbns = isbns.select(['isbn', 'cluster'])

ratings = pl.scan_csv('cleaned-ratings.csv')
ratings = ratings.select([
    pl.col('user').cast(pl.Int32),
    pl.col('isbn'),
])

joined = ratings.join(isbns, on='isbn')
joined = joined.select([
    pl.col('user'),
    pl.col('cluster').alias('item'),
])

actions = joined.groupby(['user', 'item']).agg([
    pl.col('item').count().alias('nactions')
])

_log.info('collecting results')
actions = actions.collect()

_log.info('writing %d actions to Parquet', actions.height)
actions.write_parquet('bx-cluster-actions.parquet', compression='zstd')
