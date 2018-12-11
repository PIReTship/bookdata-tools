import logging
from io import StringIO
import csv
import subprocess as sp

import numpy as np
from tqdm import tqdm
import psycopg2

from invoke import task

import support as s

_log = logging.getLogger(__name__)


@task(s.init)
def import_bx(c, force=False):
    "Import BookCrossing ratings"
    s.start('bx-ratings', force=force)
    _log.info("initializing BX schema")
    c.run('psql -f bx-schema.sql')
    _log.info("cleaning BX rating data")
    with open('data/BX-Book-Ratings.csv', 'rb') as bf:
        data = bf.read()
    barr = np.frombuffer(data, dtype='u1')
    # delete bytes that are too big
    barr = barr[barr < 128]
    # convert to LF
    barr = barr[barr != ord('\r')]
    # change delimiter to comma
    barr[barr == ord(';')] = ord(',')

    # write
    _log.info('importing BX to database')
    data = bytes(barr)
    rd = StringIO(data.decode('utf8'))

    with s.database() as dbc:
        # with dbc encapsulates a transaction
        with dbc, dbc.cursor() as cur:
            for row in tqdm(csv.DictReader(rd)):
                cur.execute('INSERT INTO bx_raw_ratings (user_id, isbn, rating) VALUES (%s, %s, %s)',
                            (row['User-ID'], row['ISBN'], row['Book-Rating']))
            s.finish('bx-ratings', dbc)


@task(s.init, s.build)
def import_az(c, force=False):
    "Import Amazon ratings"
    s.start('az-ratings', force=force)
    _log.info('Resetting Amazon schema')
    c.run('psql -f az-schema.sql')
    _log.info('Importing Amazon ratings')
    s.pipeline([
      [s.bin_dir / 'pcat', s.data_dir / 'ratings_Books.csv'],
      ['psql', '-c', '\\copy az_raw_ratings FROM STDIN (FORMAT CSV)']
    ])
    s.finish('az-ratings')


@task(s.init, s.build)
def import_gr(c, force=False):
    "Import GoodReads ratings"
    s.start('gr-data', force=force)
    _log.info('Resetting GoodReads schema')
    c.run('psql -f gr-schema.sql')
    _log.info('Importing GoodReads books')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_books.json.gz'],
      ['psql', '-c', '\\copy gr_raw_book (gr_book_data) FROM STDIN']
    ])
    _log.info('Importing GoodReads works')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_book_works.json.gz'],
      ['psql', '-c', '\\copy gr_raw_work (gr_work_data) FROM STDIN']
    ])
    _log.info('Importing GoodReads authors')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_book_authors.json.gz'],
      ['psql', '-c', '\\copy gr_raw_author (gr_author_data) FROM STDIN']
    ])
    _log.info('Importing GoodReads interactions')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_interactions.json.gz'],
      ['psql', '-c', '\\copy gr_raw_interaction (gr_int_data) FROM STDIN']
    ])
    s.finish('gr-data')


@task(s.init)
def index_az(c, force=False):
    "Index Amazon rating data"
    s.check_prereq('az-ratings')
    s.check_prereq('cluster')
    s.start('az-index', force=force)
    _log.info('building Amazon indexes')
    c.run('psql -af az-index.sql')
    s.finish('az-index')

@task(s.init)
def index_bx(c, force=False):
    "Index BookCrossing rating data"
    s.check_prereq('bx-ratings')
    s.check_prereq('cluster')
    s.start('bx-index', force=force)
    _log.info('building Amazon indexes')
    c.run('psql -af bx-index.sql')
    s.finish('bx-index')

@task(s.init)
def index_gr_books(c, force=False):
    s.check_prereq('gr-data')
    s.start('gr-index-books', force=force)
    _log.info('building GoodReads indexes')
    c.run('psql -af gr-index-books.sql')
    s.finish('gr-index-books')


@task(s.init)
def index_gr(c, force=False):
    "Index GoodReads data"
    s.check_prereq('gr-data')
    s.check_prereq('cluster')
    s.start('gr-index', force=force)
    _log.info('building GoodReads indexes')
    c.run('psql -af gr-index.sql')
    s.finish('gr-index')

@task(s.init, index_az, index_bx, index_gr)
def index(c):
    "Index all rating data"
    _log.info('done')
