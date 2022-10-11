"""
Extract BX ratings for book clusters.

Usage:
    bx-cluster-ratings.py [options]

Options:
    -v, --verbose
        Turn on debug logging.
"""

from bookdata import script_log
from docopt import docopt

import polars as pl

opts = docopt(__doc__)
_log = script_log('bx-cluster-ratings', debug=opts['--verbose'])

_log.info('setting up inputs')
isbns = pl.scan_parquet("../book-links/isbn-clusters.parquet")
isbns = isbns.select(['isbn', 'cluster'])

ratings = pl.scan_csv('cleaned-ratings.csv')
ratings = ratings.select([
    pl.col('user').cast(pl.Int32),
    pl.col('isbn'),
    pl.col('rating').cast(pl.Float32),
]).filter(pl.col('rating') > pl.lit(0))

joined = ratings.join(isbns, on='isbn')
joined = joined.select([
    pl.col('user'),
    pl.col('cluster').alias('item'),
    pl.col('rating'),
])

actions = joined.groupby(['user', 'item']).agg([
    pl.col('item').count().alias('nactions'),
    pl.col('rating').median(),
])

_log.info('collecting actions')
actions = actions.collect()

_log.info('writing %d actions to Parquet', actions.height)
actions.write_parquet('bx-cluster-ratings.parquet', compression='zstd')
