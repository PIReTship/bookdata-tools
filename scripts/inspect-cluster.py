"""
Inspect a book cluster.

Usage:
    inspect-cluster.py [options] --stats
    inspect-cluster.py [options] --records CLUSTER
    inspect-cluster.py [options] --graph CLUSTER

Options:
    -o FILE
        Write output to FILE
    CLUSTER
        The cluster number to inspect.
"""

import sys
import re
import json
from docopt import docopt

import pandas as pd

from bookdata import tracking, db, script_log


def stats(dbc, out, opts):
    "Compute statistics of the clustering"
    with dbc.cursor() as cur:
        _log.info('getting aggregate stats')
        cur.execute('SELECT COUNT(*), MAX(isbns) FROM cluster_stats')
        n_clusters, m_isbns = cur.fetchone()
        print(f'Clusters: {n_clusters}', file=out)
        print(f'Largest has {m_isbns} ISBNs', file=out)

    _log.info('computing top stats')
    print('Top clusters by size:', file=out)
    top = pd.read_sql('SELECT * FROM cluster_stats ORDER BY isbns DESC LIMIT 10', dbc)
    print(top.fillna(0), file=out)


def records(dbc, out, opts):
    "Dump ISBN records from a cluster to a CSV file"
    cluster = opts['CLUSTER']
    bc_recs = []
    _log.info('inspecting cluster %s', cluster)

    _log.info('fetching LOC records')
    bc_recs.append(pd.read_sql(f'''
        SELECT isbn, 'LOC' AS source, rec_id AS record, NULL AS work, title
        FROM locmds.book_rec_isbn
        JOIN isbn_id USING (isbn_id)
        JOIN isbn_cluster USING (isbn_id)
        LEFT JOIN locmds.book_title USING (rec_id)
        WHERE cluster = {cluster}
    ''', dbc))

    _log.info('fetching OL records')
    bc_recs.append(pd.read_sql(f'''
        SELECT isbn, 'OL' AS source,
            edition_id AS record, work_id AS work,
            title
        FROM ol.isbn_link
        JOIN isbn_id USING (isbn_id)
        JOIN isbn_cluster USING (isbn_id)
        LEFT JOIN ol.edition_title USING (edition_id)
        WHERE cluster = {cluster}
    ''', dbc))

    _log.info('fetching GR records')
    bc_recs.append(pd.read_sql(f'''
        SELECT isbn, 'GR' AS source,
            gr_book_id AS record, gr_work_id AS work,
            work_title
        FROM gr.book_isbn
        JOIN isbn_id USING (isbn_id)
        JOIN isbn_cluster USING (isbn_id)
        JOIN gr.book_ids USING (gr_book_id)
        LEFT JOIN gr.work_titles USING (gr_work_id)
        WHERE cluster = {cluster}
    ''', dbc))

    bc_recs = pd.concat(bc_recs, ignore_index=True)
    bc_recs.sort_values('isbn', inplace=True)
    _log.info('fetched %d records', len(bc_recs))

    bc_recs.to_csv(out, index=False)


def graph(dbc, out, opts):
    cluster = opts['CLUSTER']
    _log.info('exporting graph for cluster %s', cluster)

    def p(code, *args):
        print(code.format(*args), file=out)

    def node(**attrs):
        p('  node [')
        for k, v in attrs.items():
            if k == 'label':
                v = str(v)
            if v is not None:
                p('    {} {}', k, json.dumps(v))
        p('  ]')

    def edge(**attrs):
        p('  edge [')
        for k, v in attrs.items():
            if v is not None:
                p('    {} {}', k, json.dumps(v))
        p('  ]')

    with dbc.cursor() as cur:
        cur.execute('''
            CREATE TEMPORARY TABLE gc_isbns
            AS SELECT isbn_id, isbn
            FROM isbn_cluster JOIN isbn_id USING (isbn_id)
            WHERE cluster = %s
        ''', [cluster])
        p('graph [')
        p('  directed 0')

        _log.info('fetching ISBNs')
        cur.execute('SELECT * FROM gc_isbns')
        for iid, isbn in cur:
            node(id=f'i{iid}', label=isbn, category='ISBN')

        _log.info('fetching LOC records')
        cur.execute('''
            SELECT DISTINCT rec_id, title
            FROM gc_isbns
            JOIN locmds.book_rec_isbn USING (isbn_id)
            LEFT JOIN locmds.book_title USING (rec_id)
        ''')
        for rid, title in cur:
            node(id=f'l{rid}', label=rid, category='LOC', title=title)

        _log.info('fetching LOC ISBN links')
        cur.execute('''
            SELECT isbn_id, rec_id
            FROM gc_isbns
            JOIN locmds.book_rec_isbn USING (isbn_id)
        ''')
        for iid, rid in cur:
            edge(source=f'l{rid}', target=f'i{iid}')

        _log.info('fetching OL editions')
        cur.execute('''
            SELECT DISTINCT
                edition_id, edition_key,
                NULLIF(edition_data->>'title', '') AS title
            FROM gc_isbns
            JOIN ol.isbn_link USING (isbn_id)
            JOIN ol.edition USING (edition_id)
        ''')
        for eid, ek, e_title in cur:
            node(id=f'ole{eid}', label=ek, category='OLE', title=e_title)

        _log.info('fetching OL works')
        cur.execute('''
            SELECT DISTINCT
                work_id, work_key,
                NULLIF(work_data->>'title', '') AS title
            FROM gc_isbns
            JOIN ol.isbn_link USING (isbn_id)
            JOIN ol.work USING (work_id)
        ''')
        for wid, wk, w_title in cur:
            node(id=f'olw{wid}', label=wk, category='OLW', title=w_title)

        _log.info('fetching OL ISBN edges')
        cur.execute('''
            SELECT DISTINCT isbn_id, edition_id
            FROM gc_isbns
            JOIN ol.isbn_link USING (isbn_id)
        ''')
        for iid, eid in cur:
            edge(source=f'ole{eid}', target=f'i{iid}')

        _log.info('fetching OL edition/work edges')
        cur.execute('''
            SELECT DISTINCT edition_id, work_id
            FROM gc_isbns
            JOIN ol.isbn_link USING (isbn_id)
            WHERE work_id IS NOT NULL
        ''')
        for eid, wid in cur:
            edge(source=f'ole{eid}', target=f'olw{wid}')

        _log.info('fetching GR books')
        cur.execute('''
            SELECT DISTINCT isbn_id, gr_book_id
            FROM gc_isbns
            JOIN gr.book_isbn USING (isbn_id)
        ''')
        bids = set()
        for iid, bid in cur:
            if bid not in bids:
                node(id=f'grb{bid}', label=bid, category='GRB')
                bids.add(bid)
            edge(source=f'grb{bid}', target=f'i{iid}')

        _log.info('fetching GR works')
        cur.execute('''
            SELECT DISTINCT gr_work_id, work_title
            FROM gc_isbns
            JOIN gr.book_isbn USING (isbn_id)
            JOIN gr.book_ids ids USING (gr_book_id)
            LEFT JOIN gr.work_titles USING (gr_work_id)
            WHERE ids.gr_work_id IS NOT NULL
        ''')
        for wid, title in cur:
            node(id=f'grw{wid}', label=wid, category='GRW', title=title)

        _log.info('fetching GR work/edition edges')
        cur.execute('''
            SELECT DISTINCT gr_work_id, gr_book_id
            FROM gc_isbns
            JOIN gr.book_isbn USING (isbn_id)
            JOIN gr.book_ids ids USING (gr_book_id)
            WHERE ids.gr_work_id IS NOT NULL
        ''')
        for wid, bid in cur:
            edge(source=f'grw{wid}', target=f'grb{bid}')

    p(']')
    _log.info('exported graph')


_log = script_log(__name__)
opts = docopt(__doc__)

if opts['-o']:
    out = open(opts['-o'], 'w', encoding='utf8')
else:
    out = sys.stdout

with db.connect() as dbc:
    if opts['--stats']:
        stats(dbc, out, opts)
    elif opts['--records']:
        records(dbc, out, opts)
    elif opts['--graph']:
        graph(dbc, out, opts)
