"""
Usage:
    stage-status.py [-o FILE] STAGE

Options:
    -o FILE
        Write output to FILE.
    STAGE
        The stage to check.
"""

import sys
from docopt import docopt
from bookdata import db, script_log

_log = script_log(__file__)
opts = docopt(__doc__)


stage = opts.get('STAGE')
out = opts.get('-o', None)
if out is None:
    out = f'{stage}.status'

if out == '-':
    sf = sys.stdout
else:
    sf = open(out, 'w')

with db.connect() as dbc, dbc.cursor() as cur:
    cur.execute('''
        SELECT started_at, finished_at, checksum FROM stage_status WHERE stage_name = %s
    ''', [stage])
    row = cur.fetchone()
    if not row:
        _log.error('stage %s not run', stage)
        sys.exit(2)

    start, end, hash = row

    _log.info('stage %s finished at %s', stage, end)
    print('STAGE', stage, file=sf)
    print('START', start, file=sf)

    cur.execute('''
        SELECT filename, checksum
        FROM source_file
        JOIN stage_file USING (filename)
        WHERE stage_name = %s
        ORDER BY filename
    ''', [stage])
    for fn, fh in cur:
        print('SOURCE', fn, fh, file=sf)

    print('FINISH', end, file=sf)
    if hash:
        print('HASH', hash, file=sf)

sf.close()
