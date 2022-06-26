"""
Extract VIAF author genders.

Usage:
    author-genders.py [options]

Options:
    -v, --verbose
        Turn on debug logging.
"""

from bookdata import script_log
from docopt import docopt

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

opts = docopt(__doc__)
_log = script_log('author-genders', debug=opts['--verbose'])

_log.info('reading inputs')
tbl = pq.read_table('viaf.parquet', filters=[
    ('tag', '=', 375),
    ('sf_code', '=', ord('a'))
], columns=['rec_id', 'contents'])

_log.info('read %d rows', tbl.num_rows)

ids = tbl[0]
genders = tbl[1]
del tbl

_log.info('trimming genders')
genders = pc.ascii_trim_whitespace(genders)
genders = pc.ascii_lower(genders)

_log.info('dropping null values')
mask = pc.is_null(genders)
mask = pc.or_(mask, pc.equal(genders, ''))
mask = pc.invert(mask)

ids = pc.array_filter(ids, mask)
genders = pc.array_filter(genders, mask)

_log.info('reassembling table')

tbl = pa.table([ids, genders], schema=pa.schema([
    pa.field('rec_id', pa.uint32(), False),
    pa.field('gender', pa.utf8(), False),
]))

_log.info('writing %d rows to Parquet', tbl.num_rows)
pq.write_table(tbl, 'author-genders.parquet', compression='zstd', version='2.4')
