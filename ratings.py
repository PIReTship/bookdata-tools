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


@task(s.build)
def import_gr(c):
    "Import GoodReads ratings"
    print('Resetting GoodReads schema')
    c.run('psql -f gr-schema.sql')
    print('Importing GoodReads books')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_books.json.gz'],
      ['psql', '-c', '\\copy gr_raw_book (gr_book_data) FROM STDIN']
    ])
    print('Importing GoodReads works')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_book_works.json.gz'],
      ['psql', '-c', '\\copy gr_raw_work (gr_work_data) FROM STDIN']
    ])
    print('Importing GoodReads authors')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_book_authors.json.gz'],
      ['psql', '-c', '\\copy gr_raw_author (gr_author_data) FROM STDIN']
    ])
    print('Importing GoodReads interactions')
    s.pipeline([
      [s.bin_dir / 'clean-json', s.data_dir / 'goodreads_interactions.json.gz'],
      ['psql', '-c', '\\copy gr_raw_interaction (gr_int_data) FROM STDIN']
    ])
