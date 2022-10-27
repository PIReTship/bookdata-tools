"""
Extract GoodReads ratings for book clusters.

Usage:
    gr-cluster-interactions.py [options] -o FILE --ratings
    gr-cluster-interactions.py [options] -o FILE --add-actions

Options:
    -o, --output=FILE
        Write output to FILE
    --simple
        Cluster simple interactions instead of full interactions.
    --ratings
        Cluster ratings.
    --add-actions
        Cluster add-to-shelf actions (including ratings).
    --native-works
        Use GoodReads works instead of clusters.
    -v, --verbose
        Turn on debug logging.
"""

from bookdata import script_log
from bookdata.schema import ns_gr_book, ns_gr_work
from docopt import docopt

import polars as pl


def id_col_expr(opts):
    if opts['--native-works']:
        _log.info('grouping by native works')
        id_col = pl.when(
            pl.col('work_id').is_not_null()
        ).then(
            pl.col('work_id') + pl.lit(ns_gr_work.offset)
        ).otherwise(
            pl.col('book_id') + pl.lit(ns_gr_book.offset)
        )
    else:
        _log.info('grouping by integrated clusters')
        id_col = pl.col('cluster')

    return id_col


def load_input(opts):
    _log.info('setting up input')
    if opts['--simple']:
        interactions = pl.scan_parquet('gr-simple-interactions.parquet')
    else:
        interactions = pl.scan_parquet('gr-interactions.parquet')

    if opts['--ratings']:
        interactions = interactions.filter(pl.col('rating').is_not_null())

    links = pl.scan_parquet("gr-book-link.parquet")
    interactions = interactions.join(links, on='book_id')

    return interactions


def filter_and_sort(opts, interactions):
    id_col = id_col_expr(opts)
    if opts['--simple']:
        return interactions.select([
            pl.col('user_id').alias('user'),
            id_col.alias('item'),
            pl.col('rating'),
        ])
    else:
        return interactions.select([
            pl.col('user_id').alias('user'),
            id_col.alias('item'),
            (pl.col('updated').cast(pl.Int64) / pl.lit(1000)).alias('timestamp'),
            pl.col('rating'),
        ]).sort('timestamp')


def rating_aggs(opts):
    if opts['--simple']:
        return [
            pl.col('rating').median().alias('rating'),
            pl.col('item').count().alias('nratings'),
        ]
    else:
        return [
            pl.col('rating').median().alias('rating'),
            pl.col('rating').last().alias('last_rating'),
            pl.col('timestamp').min().alias('first_time'),
            pl.col('timestamp').max().alias('last_time'),
            pl.col('item').count().alias('nratings'),
        ]


def action_aggs(opts):
    if opts['--simple']:
        return [
            pl.col('item').count().alias('nactions'),
        ]
    else:
        return [
            pl.col('timestamp').min().alias('first_time'),
            pl.col('timestamp').max().alias('last_time'),
            pl.col('item').count().alias('nactions'),
        ]


def main(opts):
    interactions = load_input(opts)
    interactions = filter_and_sort(opts, interactions)

    if opts['--ratings']:
        aggs = rating_aggs(opts)
    elif opts['--add-actions']:
        aggs = action_aggs(opts)
    else:
        _log.error('invalid action mode')
        raise ValueError('no mode specified')

    actions = interactions.groupby(['user', 'item']).agg(aggs)

    if opts['--add-actions']:
        # add ratings when available
        ratings = interactions.filter(
            pl.col('rating').is_not_null()
        ).groupby(['user', 'item']).agg([
            pl.col('rating').last().alias('last_rating')
        ])
        actions = actions.join(ratings, on=['user', 'item'])

    _log.info('collecting results')
    actions = actions.collect()

    _log.info('writing %d actions to Parquet %s', actions.height, opts['--output'])
    actions.write_parquet(opts['--output'], compression='zstd')


opts = docopt(__doc__)
_log = script_log('gr-cluster-interactions', debug=opts['--verbose'])
main(opts)
