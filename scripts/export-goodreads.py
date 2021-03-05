"""
Export GoodReads-specific data from the book data tools.

Usage:
    export.py --book-ids
    export.py --work-ratings [--implicit]
"""

from pathlib import Path
from docopt import docopt
import pandas as pd

from bookdata import script_log
from bookdata import db

_log = script_log(__file__)


def export_book_ids():
    query = '''
        SELECT gr_book_rid, gr_book_id, gr_work_id, cluster AS book_id
        FROM gr.book_ids JOIN gr.book_cluster USING (gr_book_id)
        ORDER BY gr_book_rid
    '''
    _log.info('reading book IDs')
    with db.connect() as dbc:
        books = db.load_table(dbc, query)

    csv_fn = 'gr-book-ids.csv.gz'
    pq_fn = 'gr-book-ids.parquet'
    _log.info('writing CSV to %s', csv_fn)
    books.to_csv(csv_fn, index=False)
    _log.info('writing parquet to %s', pq_fn)
    books.to_parquet(pq_fn, index=False, compression='gzip')


def export_work_actions():
    path = data_dir / 'GR-I' / 'work-ratings.parquet'

    query = f'''
    SELECT gr_user_rid AS user_id,
            COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id)) AS book_id,
            COUNT(rating) AS nactions,
            MIN(EXTRACT(EPOCH FROM date_updated)) AS first_time,
            MAX(EXTRACT(EPOCH FROM date_updated)) AS last_time
     FROM gr.interaction JOIN gr.book_ids USING (gr_book_id)
     GROUP BY gr_user_rid, COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id))
     ORDER BY MIN(date_updated)
    '''
    _log.info('reading book shelf actions')
    actions = dt.load_table(query, dtype={
        'user': 'i4',
        'item': 'i4',
        'nactions': 'i4'
    })

    path.parent.mkdir(parents=True, exist_ok=True)
    _log.info('writing ratings to %s', path)
    actions.to_parquet(path, index=False)


def export_work_ratings():
    path = data_dir / 'GR-E' / 'work-ratings.parquet'

    query = f'''
    SELECT gr_user_rid AS user_id,
            COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id)) AS book_id,
            MEDIAN(rating) AS rating,
            (array_agg(rating ORDER BY date_updated DESC))[1] AS last_rating,
            MEDIAN(EXTRACT(EPOCH FROM date_updated)) AS timestamp,
            COUNT(rating) AS nratings
     FROM gr.interaction JOIN gr.book_ids USING (gr_book_id)
     WHERE rating > 0
     GROUP BY gr_user_rid, COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id))
     ORDER BY MIN(date_updated)
    '''
    _log.info('reading book ratings')
    ratings = dt.load_table(query, dtype={
        'user': 'i4',
        'item': 'i4',
        'rating': 'f4',
        'nactions': 'i4'
    })

    path.parent.mkdir(parents=True, exist_ok=True)
    _log.info('writing ratings to %s', path)
    ratings.to_parquet(path, index=False)


args = docopt(__doc__)

if args['--book-ids']:
    export_book_ids()
if args['--work-ratings']:
    if args['--implicit']:
        export_work_actions()
    else:
        export_work_ratings()
