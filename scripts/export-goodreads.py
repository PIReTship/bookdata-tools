"""
Export GoodReads-specific data from the book data tools.

Usage:
    export.py --book-ids
    export.py --work-titles
    export.py --work-ratings [--implicit]
"""

from pathlib import Path
from docopt import docopt
import pandas as pd

from bookdata import script_log
from bookdata import db

_log = script_log('export-goodreads')


def export_book_ids():
    query = '''
        SELECT gr_book_rid, gr_book_id, gr_work_id, cluster AS book_id
        FROM gr.book_ids JOIN gr.book_cluster USING (gr_book_id)
        ORDER BY gr_book_rid
    '''

    with db.connect() as dbc:
        _log.info('reading book IDs')
        books = db.load_table(dbc, query)

    csv_fn = 'gr-book-ids.csv.gz'
    pq_fn = 'gr-book-ids.parquet'
    _log.info('writing CSV to %s', csv_fn)
    books.to_csv(csv_fn, index=False)
    _log.info('writing parquet to %s', pq_fn)
    books.to_parquet(pq_fn, index=False, compression='gzip')


def export_work_titles():
    query = f'''
        SELECT gr_work_rid, gr_work_id, work_title
        FROM gr.work_title
        ORDER BY gr_work_rid
    '''

    with db.connect() as dbc:
        _log.info('reading work titles')
        books = db.load_table(dbc, query)

    pq_fn = 'gr-work-titles.parquet'
    _log.info('writing parquet to %s', pq_fn)
    books.to_parquet(pq_fn, index=False, compression='brotli')


def export_work_actions():
    query = '''
    SELECT gr_user_rid AS user,
            COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id)) AS item,
            COUNT(rating) AS nactions,
            MIN(EXTRACT(EPOCH FROM date_updated)) AS first_time,
            MAX(EXTRACT(EPOCH FROM date_updated)) AS last_time
     FROM gr.interaction JOIN gr.book_ids USING (gr_book_id)
     GROUP BY gr_user_rid, COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id))
     ORDER BY MIN(date_updated)
    '''

    with db.connect() as dbc:
        _log.info('reading book shelf actions')
        actions = db.load_table(dbc, query, dtype={
            'user': 'i4',
            'item': 'i4',
            'nactions': 'i4'
        })

    _log.info('writing actions')
    actions.to_parquet('gr-work-actions.parquet', index=False, compression='brotli')


def export_work_ratings():
    query = '''
    SELECT gr_user_rid AS user,
            COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id)) AS item,
            MEDIAN(rating) AS rating,
            (array_agg(rating ORDER BY date_updated DESC))[1] AS last_rating,
            MEDIAN(EXTRACT(EPOCH FROM date_updated)) AS timestamp,
            COUNT(rating) AS nratings
     FROM gr.interaction JOIN gr.book_ids USING (gr_book_id)
     WHERE rating > 0
     GROUP BY gr_user_rid, COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id))
     ORDER BY MIN(date_updated)
    '''

    with db.connect() as dbc:
        _log.info('reading book ratings')
        ratings = db.load_table(dbc, query, dtype={
            'user': 'i4',
            'item': 'i4',
            'rating': 'f4',
            'last_rating': 'f4',
            'nratings': 'i4'
        })

    _log.info('writing ratings')
    ratings.to_parquet('gr-work-ratings.parquet', index=False, compression='brotli')


args = docopt(__doc__)

if args['--book-ids']:
    export_book_ids()
if args['--work-titles']:
    export_work_titles()
if args['--work-ratings']:
    if args['--implicit']:
        export_work_actions()
    else:
        export_work_ratings()
