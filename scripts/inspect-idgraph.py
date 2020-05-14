"""
Inspect a book cluster.

Usage:
    inspect-idgraph.py [options] --stats
    inspect-idgraph.py [options] --records CLUSTER
    inspect-idgraph.py [options] --graph CLUSTER
    inspect-idgraph.py [options] --full-graph

Options:
    -o FILE
        Write output to FILE
    -f, --format FMT
        Output in format FMT.
    CLUSTER
        The cluster number to inspect.
"""

import sys
import re
import json
from xml.etree import ElementTree as etree
from textwrap import dedent as d
from docopt import docopt

import pandas as pd

from bookdata import tracking, db, script_log
from bookdata.graph import GraphLoader


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
        LEFT JOIN gr.work_title USING (gr_work_id)
        WHERE cluster = {cluster}
    ''', dbc))

    bc_recs = pd.concat(bc_recs, ignore_index=True)
    bc_recs.sort_values('isbn', inplace=True)
    _log.info('fetched %d records', len(bc_recs))

    bc_recs.to_csv(out, index=False)


def graph(opts):
    cluster = opts['CLUSTER']
    _log.info('exporting graph for cluster %s', cluster)

    gl = GraphLoader()
    with db.engine().connect() as cxn:
        gl.set_cluster(cluster, cxn)
        g = gl.load_graph(cxn, True)

    ofn = opts['-o']
    _log.info('saving graph to %s', ofn)
    g.save(ofn)


def full_graph(opts):
    gl = GraphLoader()
    with db.engine().connect() as cxn:
        g = gl.load_minimal_graph(cxn)


    ofn = opts['-o']
    _log.info('saving graph to %s', ofn)
    g.save(ofn)


_log = script_log(__name__)
opts = docopt(__doc__)

if opts['--full-graph']:
    full_graph(opts)
elif opts['--graph']:
    graph(opts)
else:
    if opts['-o']:
        out = open(opts['-o'], 'w', encoding='utf8')
    else:
        out = sys.stdout
    with db.connect() as dbc:
        if opts['--stats']:
            stats(dbc, out, opts)
        elif opts['--records']:
            records(dbc, out, opts)
