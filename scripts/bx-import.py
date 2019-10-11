"""
Import BookCrossing ratings, with data cleaning for invalid characters.

Usage:
    bx-import.py [-T <file>] <file>

Options:
    -T FILE
        Write transcript to FILE [default: bx-import.transcript]
"""

import hashlib
from bookdata import script_log, db
from docopt import docopt

import numpy as np
from tqdm import tqdm
from io import StringIO
import csv

_log = script_log(__name__)
opts = docopt(__doc__)
src_file = opts.get('<file>')
tx_file = open(opts.get('-T'), 'w')

_log.info("cleaning BX rating data")
with open(src_file, 'rb') as bf:
    data = bf.read()
in_chk = hashlib.sha1(data).hexdigest()

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

with db.connect() as dbc:
    print('IMPORT TO bx.raw_ratings', file=tx_file)
    print('READ', src_file, in_chk, file=tx_file)
    # we're going to hash the data we insert
    dh = hashlib.md5()
    # with dbc encapsulates a transaction
    with dbc, dbc.cursor() as cur:
        db.start_stage(cur, 'bx-ratings')
        db.record_file(cur, src_file, in_chk, 'bx-ratings')
        n = 0
        for row in tqdm(csv.DictReader(rd)):
            uid = row['User-ID']
            isbn = row['ISBN']
            rating = row['Book-Rating']
            cur.execute('INSERT INTO bx.raw_ratings (user_id, isbn, rating) VALUES (%s, %s, %s)',
                        (uid, isbn, rating))
            dh.update(f'{uid}\t{isbn}\t{rating}\n'.encode('utf8'))
            n += 1
        db.finish_stage(cur, 'bx-ratings', key=dh.hexdigest())
        print('INSERTED', n, dh.hexdigest(), file=tx_file)

tx_file.close()
