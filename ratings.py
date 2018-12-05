from io import StringIO
import csv
import subprocess as sp

import numpy as np
from tqdm import tqdm
import psycopg2

from invoke import task

import support as s


@task
def import_bx(c):
    "Import BookCrossing ratings"
    print("initializing BX schema")
    c.run('psql -f bx-schema.sql')
    print("cleaning BX rating data")
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
    print('importing BX to database')
    data = bytes(barr)
    rd = StringIO(data.decode('utf8'))
    dbc = psycopg2.connect("")
    try:
        # with dbc encapsulates a transaction
        with dbc, dbc.cursor() as cur:
            for row in tqdm(csv.DictReader(rd)):
                cur.execute('INSERT INTO bx_raw_ratings (user_id, isbn, rating) VALUES (%s, %s, %s)',
                            (row['User-ID'], row['ISBN'], row['Book-Rating']))
    finally:
        dbc.close()


@task(s.build)
def import_az(c):
    "Import Amazon ratings"
    print('Resetting Amazon schema')
    c.run('psql -f az-schema.sql')
    print('Importing Amazon ratings')
    s.pipeline([
      [s.bin_dir / 'pcat', s.data_dir / 'ratings_Books.csv'],
      ['psql', '-c', '\\copy az_raw_ratings FROM STDIN (FORMAT CSV)']
    ])
