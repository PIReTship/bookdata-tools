"""
Extract and pretty-print JSON data from the database.

Usage:
    inspect-json.py --gr-work [ID...]
    inspect-json.py --gr-book [ID...]
    inspect-json.py --ol-edition [ID...]
    inspect-json.py --ol-work [ID...]
"""

import sys
import re
import json
from docopt import docopt
from bookdata import tracking, db, script_log

class GR:
    def __init__(self, type):
        self.type = type

    def __call__(self, ids):
        with db.connect() as dbc, dbc.cursor() as cur:
            if ids:
                return [self._id_rec(cur, r) for r in ids]
            else:
                return [self._top_rec(cur)]

    def _id_rec(self, cur, id):
        t = self.type
        _log.info('fetching %s %s', t, id)
        q = f'''
            SELECT gr_{t}_data
            FROM gr.raw_{t} JOIN gr.{t}_ids USING (gr_{t}_rid)
            WHERE gr_{t}_id = %s
        '''
        cur.execute(q, [id])
        rec = cur.fetchone()
        if rec is None:
            _log.error('%s %s not found', t, id)
        else:
            return rec[0]

    def _top_rec(self, cur):
        t = self.type
        _log.info('fetching one %s', t)
        q = f'SELECT gr_{t}_data FROM gr.raw_{t} LIMIT 1'
        cur.execute(q)
        data, = cur.fetchone()
        _log.debug('got %r', data)
        return data


class OL:
    def __init__(self, type):
        self.type = type

    def __call__(self, ids):
        with db.connect() as dbc, dbc.cursor() as cur:
            if ids:
                return [self._id_rec(cur, r) for r in ids]
            else:
                return [self._top_rec(cur)]

    def _id_rec(self, cur, id):
        t = self.type
        _log.info('fetching %s %s', t, id)
        q = f'''
            SELECT {t}_data
            FROM ol.{t}
            WHERE {t}_key = %s
        '''
        cur.execute(q, [id])
        rec = cur.fetchone()
        if rec is None:
            _log.error('%s %s not found', t, id)
        else:
            return rec[0]

    def _top_rec(self, cur):
        t = self.type
        _log.info('fetching one %s', t)
        q = f'SELECT {t}_data FROM ol.{t} LIMIT 1'
        cur.execute(q)
        data, = cur.fetchone()
        _log.debug('got %r', data)
        return data


__gr_work = GR('work')
__gr_book = GR('book')
__ol_edition = OL('edition')
__ol_work = OL('work')

_log = script_log(__name__)
opts = docopt(__doc__)

rec_ids = opts.get('ID', None)

recs = None
for k in opts.keys():
    fn = k.replace('-', '_')
    if k.startswith('--') and opts[k] and fn in globals():
        f = globals()[fn]
        recs = f(rec_ids)

if recs is None:
    _log.error('could not find an operation to perform')

for rec in recs:
    print(json.dumps(rec, indent=2))
