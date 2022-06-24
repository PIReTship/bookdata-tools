"""
Extract GoodReads ratings for book clusters.

Usage:
    gr-cluster-ratings.py [options]

Options:
    -v, --verbose
        Turn on debug logging.
"""

from bookdata import script_log
from bookdata.schema import ns_gr_book, ns_gr_work
from docopt import docopt

import polars as pl

opts = docopt(__doc__)
_log = script_log('gr-cluster-ratings', debug=opts['--verbose'])

_log.info('setting up input')
links = pl.scan_parquet("gr-book-link.parquet")

ratings = pl.scan_parquet('gr-interactions.parquet')
ratings = ratings.filter(pl.col('rating').is_not_null())
ratings = ratings.join(links, on='book_id')
ratings = ratings.select([
    pl.col('user_id').alias('user'),
    pl.col('cluster').alias('item'),
    (pl.col('updated').cast(pl.Int64) / pl.lit(1000)).alias('timestamp'),
    pl.col('rating'),
]).sort('timestamp')

actions = ratings.groupby(['user', 'item']).agg([
    pl.col('rating').median().alias('rating'),
    pl.col('rating').last().alias('last_rating'),
    pl.col('timestamp').min().alias('first_time'),
    pl.col('timestamp').max().alias('last_time'),
    pl.col('item').count().alias('nratings'),
])

_log.info('collecting results')
actions = actions.collect()

_log.info('writing %d actions to Parquet', actions.height)
actions.write_parquet('gr-cluster-ratings.parquet', compression='zstd')
