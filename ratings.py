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
    s.psql(c, 'bx-schema.sql')
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
                cur.execute('INSERT INTO bx.raw_ratings (user_id, isbn, rating) VALUES (%s, %s, %s)',
                            (row['User-ID'], row['ISBN'], row['Book-Rating']))
            s.finish('bx-ratings', dbc)


@task(s.init, s.build)
def import_az(c, force=False):
    "Import Amazon ratings"
    s.start('az-ratings', force=force)
    _log.info('Resetting Amazon schema')
    s.psql(c, 'az-schema.sql')
    _log.info('Importing Amazon ratings')
    s.pipeline([
      [s.bdtool, 'pcat', s.data_dir / 'ratings_Books.csv'],
      ['psql', '-c', '\\copy az.raw_ratings FROM STDIN (FORMAT CSV)']
    ])
    s.finish('az-ratings')


@task(s.init)
def index_az(c, force=False):
    "Index Amazon rating data"
    s.check_prereq('az-ratings')
    s.check_prereq('cluster')
    s.start('az-index', force=force)
    _log.info('building Amazon indexes')
    s.psql(c, 'az-index.sql')
    s.finish('az-index')

@task(s.init)
def index_bx(c, force=False):
    "Index BookCrossing rating data"
    s.check_prereq('bx-ratings')
    s.check_prereq('cluster')
    s.start('bx-index', force=force)
    _log.info('building BX indexes')
    s.psql(c, 'bx-index.sql')
    s.finish('bx-index')

@task(s.init, index_az, index_bx)
def index(c):
    "Index all rating data"
    _log.info('done')


@task(s.init, s.build)
def record_files(c):
    files = ['ratings_Books.csv', 'BX-Book-Ratings.csv']
    files = [s.data_dir / f for f in files]
    s.booktool(c, 'hash', *files)
