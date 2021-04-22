"""
Output gender statistics.

Usage:
    gender-tool.py --stats (--mds|--lcnaf) [-o FILE]
    gender-tool.py --import-codes [-T transcript] FILE

Options:
    -o FILE
        Write gender stats to FILE
"""

import sys
from docopt import docopt

import pandas as pd

from sqlalchemy.dialects import postgresql
from bookdata import db
from bookdata import script_log
from bookdata import tracking

_log = script_log(__file__)


def gender_stats_mds(opts):
    gs_q = '''
    SELECT gender, COUNT(rec_id) AS mds_recs
    FROM locmds.author_gender
    GROUP BY gender
    ORDER BY mds_recs DESC
    '''

    _log.info('retrieving gender statistics')
    stats = pd.read_sql_query(gs_q, db.db_url())
    _log.info('found %d gender records', len(stats))

    if opts['-o']:
        stats.to_csv(opts['-o'], index=False)
    else:
        print(stats)


def gender_stats_lcnaf(opts):
    gs_q = '''
    SELECT label AS gender, entity_count AS recs
    FROM locid.gender_stats
    ORDER BY entity_count DESC
    '''

    _log.info('retrieving gender statistics')
    stats = pd.read_sql_query(gs_q, db.db_url())
    _log.info('found %d gender records', len(stats))


    if opts['-o']:
        stats.to_csv(opts['-o'], index=False)
    else:
        print(stats)


def import_codes(opts):
    if opts['-T']:
        tx = open(opts['-T'], 'w')
    else:
        tx = sys.stdout

    file = opts['FILE']
    STAGE = 'gender-codes'
    with db.connect() as dbc:
        tracking.begin_stage(dbc, STAGE)
        tracking.record_dep(dbc, STAGE, 'loc-id-author-index')
        _log.info('reading %s', file)
        hash = tracking.hash_and_record_file(dbc, file, STAGE)
        print(f'READ {file} {hash}', file=tx)
        codes = pd.read_csv(file)
        _log.info('read codes:\n%s', codes)

        _log.info('writing table')
        codes.to_sql('gender_codes', db.db_url(), 'locid', if_exists='replace', dtype={
            'gender_uuid': postgresql.UUID
        })
        print('TABLE locid.gender_codes', file=tx)
        _log.info('checking for completeness')
        miss_q = '''
            SELECT gs.gender_uuid, gs.node_iri, gs.label, gs.entity_count
            FROM locid.gender_stats gs
            LEFT JOIN locid.gender_codes gc USING (gender_uuid)
            WHERE gc.node_iri IS NULL
        '''
        missed = pd.read_sql_query(miss_q, db.db_url())
        if len(missed):
            _log.error('missing codes for %d genders:%s', len(missed), missed)
            sys.exit(1)
        else:
            _log.info('all genders accounted for')
            tracking.end_stage(dbc, STAGE, hash)



opts = docopt(__doc__)
if opts['--stats']:
    if opts['--mds']:
        gender_stats_mds(opts)
    elif opts['--lcnaf']:
        gender_stats_lcnaf(opts)
elif opts['--import-codes']:
    import_codes(opts)
