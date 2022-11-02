"""
Compute data integration statistics.

Usage:
    integration-stats.py [options]

Options:
    -s, --source=SOURCE
        Specify a specific source.
    -v, --verbose
        Turn on debug logging.
"""

from bookdata import script_log
from docopt import docopt
import tomli

import polars as pl

DATASETS = {
    'BX-I': '../bx/bx-cluster-actions.parquet',
    'BX-E': '../bx/bx-cluster-ratings.parquet',
    'AZ': '../az2014/az-cluster-ratings.parquet',
    'GR-I': '../goodreads/%SOURCE%/gr-cluster-actions.parquet',
    'GR-E': '../goodreads/%SOURCE%/gr-cluster-ratings.parquet',
}


def scan_genders():
    _log.info('scanning genders')
    return pl.scan_parquet('cluster-genders.parquet')


def scan_loc(genders):
    _log.info("scanning LOC books")
    books = pl.scan_parquet('../loc-mds/book-isbn-ids.parquet')
    clusters = pl.scan_parquet('isbn-clusters.parquet')
    books = books.join(clusters, on='isbn_id')

    bg = books.join(genders, on='cluster')
    bg = bg.groupby('gender').agg([
        pl.col('cluster').n_unique().alias('n_books'),
    ]).select([
        pl.lit('LOC-MDS').alias('dataset'),
        pl.col('gender'),
        pl.col('n_books'),
        pl.lit(None, pl.UInt32).alias('n_actions')
    ])
    _log.debug('LOC schema: %s', bg.schema)
    return bg


def scan_actions(config, genders, data):
    fn = DATASETS[data]
    if data.startswith('GR'):
        fn = fn.replace('%SOURCE%', config['goodreads']['interactions'])

    _log.info('scanning data %s from %s', data, fn)
    actions = pl.scan_parquet(fn)
    ga = actions.join(genders, left_on='item', right_on='cluster')
    ga = ga.groupby('gender').agg([
        pl.col('item').n_unique().alias('n_books'),
        pl.col('item').count().alias('n_actions'),
    ]).select([
        pl.lit(data).alias('dataset'),
        pl.col('gender'),
        pl.col('n_books'),
        pl.col('n_actions')
    ])
    _log.debug('%s schema: %s', data, ga.schema)
    return ga


def main(opts):
    with open('../config.toml', 'rb') as cf:
        config = tomli.load(cf)

    genders = scan_genders()
    loc_books = scan_loc(genders)
    actions = [scan_actions(config, genders, ds) for ds in DATASETS.keys()]
    stats = pl.concat([loc_books] + actions)

    _log.info('collecting results')
    stats = stats.collect()

    _log.info('saving results')
    stats.write_csv('gender-stats.csv')


opts = docopt(__doc__)
_log = script_log('edition-isbn-ids', debug=opts['--verbose'])
main(opts)
