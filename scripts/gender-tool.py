"""
Output gender statistics.

Usage:
    gender-stats.py --stats [-o FILE]

Options:
    -o FILE
        Write gender stats to FILE
"""

import sys
from docopt import docopt

import pandas as pd

from bookdata import db
from bookdata import script_log

_log = script_log(__file__)


def gender_stats(opts):
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


opts = docopt(__doc__)
if opts['--stats']:
    gender_stats(opts)
