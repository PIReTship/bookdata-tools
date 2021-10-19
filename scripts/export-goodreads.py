"""
Export GoodReads-specific data from the book data tools.

Usage:
    export.py --book-ids
    export.py --work-titles
    export.py --work-authors
    export.py --work-genres
    export.py --work-genders
    export.py --work-ratings
    export.py --work-actions
"""

from pathlib import Path
from docopt import docopt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from bookdata import script_log
from bookdata import db

_log = script_log('export-goodreads')


def export_book_ids():
    query = '''
        SELECT gr_book_rid, gr_book_id, gr_work_id, cluster
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
    books.to_parquet(pq_fn, index=False)


def export_work_titles():
    query = f'''
        SELECT gr_work_id AS work_id, gr_work_rid, work_title
        FROM gr.work_title
        ORDER BY gr_work_rid
    '''

    with db.connect() as dbc:
        _log.info('reading work titles')
        books = db.load_table(dbc, query)

    pq_fn = 'gr-work-titles.parquet'
    _log.info('writing parquet to %s', pq_fn)
    books.to_parquet(pq_fn, index=False)
    _log.info('writing CSV')
    books.to_csv('gr-work-titles.csv.gz', index=False)


def export_work_genres():
    query = f'''
        SELECT gr_work_id AS work_id, genre, sum(score::int) AS score
        FROM gr.book_ids JOIN gr.book_genres USING (gr_book_rid)
        GROUP BY work_id, genre
        ORDER BY work_id, genre
    '''

    with db.connect() as dbc:
        _log.info('reading work genres')
        genres = db.load_table(dbc, query)

    pq_fn = 'gr-work-genres.parquet'
    _log.info('writing parquet to %s', pq_fn)
    genres.to_parquet(pq_fn, index=False, compression='brotli')
    _log.info('writing CSV')
    genres.to_csv('gr-work-genres.csv.gz', index=False)


def export_work_genders():
    query = f'''
        SELECT DISTINCT gr_work_id AS work_id, cluster, COALESCE(gender, 'no-book') AS gender
        FROM gr.book_ids
        LEFT JOIN gr.book_isbn USING (gr_book_id)
        LEFT JOIN isbn_cluster USING (isbn_id)
        LEFT JOIN cluster_first_author_gender USING (cluster)
        ORDER BY work_id
    '''

    with db.connect() as dbc:
        _log.info('reading work genders')
        genders = db.load_table(dbc, query)

    pq_fn = 'gr-work-genders.parquet'
    _log.info('writing parquet to %s', pq_fn)
    genders.to_parquet(pq_fn, index=False, compression='brotli')
    _log.info('writing CSV')
    genders.to_csv('gr-work-genders.csv.gz', index=False)


def export_work_authors():
    query = f'''
        WITH
            pairs AS (SELECT DISTINCT gr_work_id AS work_id, gr_author_id
                      FROM gr.book_ids JOIN gr.book_authors USING (gr_book_rid)
                      WHERE author_role = '' AND gr_work_id IS NOT NULL)
        SELECT work_id, gr_author_id AS author_id, author_name
        FROM pairs JOIN gr.author_info USING (gr_author_id)
        ORDER BY work_id
    '''

    with db.connect() as dbc:
        _log.info('reading work authors')
        books = db.load_table(dbc, query)

    pq_fn = 'gr-work-authors.parquet'
    _log.info('writing parquet to %s', pq_fn)
    books.to_parquet(pq_fn, index=False, compression='brotli')
    _log.info('writing CSV')
    books.to_csv('gr-work-authors.csv.gz', index=False)


def export_work_actions():
    query = '''
    SELECT gr_user_rid AS user, gr_work_id AS item,
            COUNT(rating) AS nactions,
            MIN(EXTRACT(EPOCH FROM date_updated)) AS first_time,
            MAX(EXTRACT(EPOCH FROM date_updated)) AS last_time
     FROM gr.interaction JOIN gr.book_ids USING (gr_book_id)
     WHERE gr_work_id IS NOT NULL
     GROUP BY gr_user_rid, gr_work_id
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
    actions.to_parquet('gr-work-actions.parquet', index=False,
                       compression='zstd', compression_level=5)


def export_work_ratings():
    query = '''
    SELECT gr_user_rid AS user, gr_work_id AS item,
            MEDIAN(rating) AS rating,
            (array_agg(rating ORDER BY date_updated DESC))[1] AS last_rating,
            MEDIAN(EXTRACT(EPOCH FROM date_updated)) AS timestamp,
            COUNT(rating) AS nratings
     FROM gr.interaction JOIN gr.book_ids USING (gr_book_id)
     WHERE rating > 0 AND gr_work_id IS NOT NULL
     GROUP BY gr_user_rid, gr_work_id
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
    ratings.to_parquet('gr-work-ratings.parquet', index=False,
                       compression='zstd', compression_level=5)


args = docopt(__doc__)

if args['--book-ids']:
    export_book_ids()
if args['--work-titles']:
    export_work_titles()
if args['--work-authors']:
    export_work_authors()
if args['--work-genres']:
    export_work_genres()
if args['--work-genders']:
    export_work_genders()
if args['--work-ratings']:
    export_work_ratings()
if args['--work-actions']:
    export_work_actions()
