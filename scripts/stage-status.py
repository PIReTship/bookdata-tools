"""
Usage:
    stage-status.py [options] STAGE

Options:
    --timestamps
        Include timestamps in stage status.
    -o FILE
        Write output to FILE.
    STAGE
        The stage to check.
"""

import sys
from docopt import docopt
from bookdata import db, script_log

_log = script_log(__name__)
opts = docopt(__doc__)

timestamps = opts.get('--timestamps')

stage = opts.get('STAGE')
out = opts.get('-o', None)
if out is None:
    out = f'{stage}.status'

if out == '-':
    sf = sys.stdout
else:
    sf = open(out, 'w')

with db.connect() as dbc:
    # initialize database, in case nothing has been run
    with dbc, dbc.cursor() as cur:
        cur.execute(db.meta_schema)

    # get the status
    with dbc, dbc.cursor() as cur:
        cur.execute('''
            SELECT started_at, finished_at, stage_key FROM stage_status WHERE stage_name = %s
        ''', [stage])
        row = cur.fetchone()
        if not row:
            _log.error('stage %s not run', stage)
            sys.exit(2)

        start, end, key = row

        _log.info('stage %s finished at %s', stage, end)
        print('STAGE', stage, file=sf)
        if timestamps:
            print('START', start, file=sf)

        cur.execute('''
            SELECT dep_name, dep_key
            FROM stage_dep
            WHERE stage_name = %s
            ORDER BY dep_name
        ''', [stage])
        for dn, dk in cur:
            print('DEP', dn, dk, file=sf)

        cur.execute('''
            SELECT filename, COALESCE(link.checksum, src.checksum)
            FROM source_file src
            JOIN stage_file link USING (filename)
            WHERE stage_name = %s
            ORDER BY filename
        ''', [stage])
        for fn, fh in cur:
            print('SOURCE', fn, fh, file=sf)

        if timestamps:
            print('FINISH', end, file=sf)
        if key:
            print('KEY', key, file=sf)

sf.close()
