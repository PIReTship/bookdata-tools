"""
Extract LOC ISBN IDs.

Usage:
    book-isbn-ids.py [options]

Options:
    -v, --verbose
        Turn on debug logging.
"""

from bookdata import script_log
from docopt import docopt

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

opts = docopt(__doc__)
_log = script_log('book-isbn-ids', debug=opts['--verbose'])

_log.info('setting up inputs')
isbns = pl.scan_parquet('../book-links/all-isbns.parquet')
books = pl.scan_parquet('book-isbns.parquet')

books = books.join(isbns, on='isbn').select(['rec_id', 'isbn_id'])

_log.info('collecting results')
books = books.collect()

_log.info('saving {} book ISBNs to Parquet', books.height)

# non-null
table = books.to_arrow()
table = table.cast(pa.schema([
    pa.field(fn, ft, False)
    for (fn, ft) in zip(table.schema.names, table.schema.types)
]))

# all_isbns.write_parquet('book-isbn-ids.parquet', compression='zstd')
pq.write_table(table, 'book-isbn-ids.parquet', compression='zstd')

_log.info('finished')
