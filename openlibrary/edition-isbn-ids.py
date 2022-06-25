"""
Extract OpenLibrary Edition ISBN IDs.

Usage:
    edition-isbn-ids.py [options]

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
_log = script_log('edition-isbn-ids', debug=opts['--verbose'])

_log.info('setting up inputs')
isbns = pl.scan_parquet('../book-links/all-isbns.parquet')
editions = pl.scan_parquet('edition-isbns.parquet')

editions = editions.join(isbns, on='isbn').select(['edition', 'isbn_id'])
editions = editions.unique(False)

_log.info('collecting results')
editions = editions.collect()

_log.info('saving %d edition ISBNs to Parquet', editions.height)

# non-null
table = editions.to_arrow()
table = table.cast(pa.schema([
    pa.field(fn, ft, False)
    for (fn, ft) in zip(table.schema.names, table.schema.types)
]))

pq.write_table(table, 'edition-isbn-ids.parquet', compression='zstd', version='2.4')

_log.info('finished')
