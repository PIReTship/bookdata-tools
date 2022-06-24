"""
Extract GoodReads ISBN IDs.

Usage:
    book-isbn-ids.py [options]

Options:
    -o, --output=FILE
        Write output to FILE
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
books = pl.scan_parquet('gr-book-ids.parquet')

isbn10s = books.filter(
    pl.col('isbn10').is_not_null()
).join(isbns, left_on='isbn10', right_on='isbn')
isbn13s = books.filter(
    pl.col('isbn13').is_not_null()
).join(isbns, left_on='isbn13', right_on='isbn')
asins = books.filter(
    pl.col('asin').is_not_null()
).join(isbns, left_on='asin', right_on='isbn')

all_isbns = pl.concat([
    frame.select(['book_id', 'isbn_id'])
    for frame in [isbn10s, isbn13s, asins]
]).distinct(False)

_log.info('collecting results')
all_isbns = all_isbns.collect()

# non-null
table = all_isbns.to_arrow()
table = table.cast(pa.schema([
    pa.field(fn, ft, False)
    for (fn, ft) in zip(table.schema.names, table.schema.types)
]))

_log.info('writing %d records to output', all_isbns.height)
# all_isbns.write_parquet('book-isbn-ids.parquet', compression='zstd')
pq.write_table(table, 'book-isbn-ids.parquet', compression='zstd')

_log.info('finished')
