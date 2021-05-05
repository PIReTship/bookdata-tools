"""
Import BookCrossing ratings, with data cleaning for invalid characters.

Usage:
    bx-import.py <zip> <output>

Options:
    <zip>
        The zip file to read.
    <output>
        The output file to write.
"""

from bookdata import script_log
from docopt import docopt

import numpy as np
import pandas as pd
from io import BytesIO
from zipfile import ZipFile

_log = script_log(__name__)
opts = docopt(__doc__)

_log.info("extracting BX rating data")
with ZipFile(opts['<zip>'], 'r') as zf:
    with zf.open('BX-Book-Ratings.csv') as f:
        data = f.read()


_log.info("cleaning BX rating data")
barr = np.frombuffer(data, dtype='u1')
# delete bytes that are too big
barr = barr[barr < 128]
# convert to LF
barr = barr[barr != ord('\r')]
# change delimiter to comma
barr[barr == ord(';')] = ord(',')
data = bytes(barr)

_log.info("parsing BX rating data")
rd = BytesIO(data)
df = pd.read_csv(rd)
df = df.rename(columns={
    'User-ID': 'user',
    'ISBN': 'isbn',
    'Book-Rating': 'rating'
})
df['isbn'] = df['isbn'].str.strip().str.upper()

_log.info('writing output file')
df.to_csv(opts['<output>'], index=False)
