"""
Extract Amazon ratings for book clusters.

Usage:
    az-cluster-ratings.py [options]

Options:
    -v, --verbose
        Turn on debug logging.
"""

from bookdata import script_log
from docopt import docopt

import polars as pl

opts = docopt(__doc__)
_log = script_log('az-cluster-ratings', debug=opts['--verbose'])

_log.info('setting up input')
isbns = pl.scan_parquet("../book-links/isbn-clusters.parquet")
isbns = isbns.select(['isbn', 'cluster'])

ratings = pl.scan_parquet('ratings.parquet')

joined = ratings.join(isbns, left_on='asin', right_on='isbn')
joined = joined.select([
    pl.col('user'),
    pl.col('cluster').alias('item'),
    pl.col('rating'),
    pl.col('timestamp')
]).sort('timestamp')

actions = joined.groupby(['user', 'item']).agg([
    pl.col('rating').median().alias('rating'),
    pl.col('rating').last().alias('last_rating'),
    pl.col('timestamp').min().alias('first_time'),
    pl.col('timestamp').max().alias('last_time'),
    pl.col('item').count().alias('nratings'),
])

_log.info('collecting results')
actions = actions.collect()

_log.info('writing %d actions to Parquet', actions.height)
actions.write_parquet('az-cluster-ratings.parquet', compression='zstd')
