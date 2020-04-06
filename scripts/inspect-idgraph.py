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


class GMLWriter:
    def __init__(self, out):
        self.output = out
        self._n_attrs = set(['id'])

    def _p(self, code, *args):
        print(code.format(*args), file=self.output)

    def node_attr(self, name):
        self._n_attrs.add(name)

    def start(self):
        self._p('graph [')
        self._p('  directed 0')

    def finish(self):
        self._p(']')

    def node(self, **attrs):
        self._p('  node [')
        for k, v in attrs.items():
            if k not in self._n_attrs:
                raise RuntimeError('unknown node attribute ' + k)
            if k == 'label':
                v = str(v)
            if v is not None:
                self._p('    {} {}', k, json.dumps(v))
        self._p('  ]')

    def edge(self, **attrs):
        self._p('  edge [')
        for k, v in attrs.items():
            if v is not None:
                self._p('    {} {}', k, json.dumps(v))
        self._p('  ]')


class GraphMLWriter:
    _g_started = False

    def __init__(self, out):
        self.output = out
        self.tb = etree.TreeBuilder()
        self._ec = 0

    def node_attr(self, name, type='string'):
        self.tb.start('key', {
            'id': name,
            'for': 'node',
            'attr.name': name,
            'attr.type': type
        })
        self.tb.end('key')

    def start(self):
        self.tb.start('graphml', {
            'xmlns': 'http://graphml.graphdrawing.org/xmlns',
            '{http://www.w3.org/2001/XMLSchema-instance}schemaLocation': d('''
                http://graphml.graphdrawing.org/xmlns
                http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd
            ''').strip(),
        })

    def finish(self):
        self.tb.end('graph')
        self.tb.end('graphml')
        elt = self.tb.close()
        tree = etree.ElementTree(elt)
        tree.write(self.output, encoding='unicode')

    def node(self, id, **attrs):
        if not self._g_started:
            self.tb.start('graph', {
                'edgedefault': 'undirected'
            })
            self._g_started = True

        self.tb.start('node', {
            'id': id
        })
        for k, v in attrs.items():
            if v is not None:
                self.tb.start('data', {'key': k})
                self.tb.data(str(v))
                self.tb.end('data')
        self.tb.end('node')

    def edge(self, source, target):
        self._ec += 1
        eid = self._ec
        self.tb.start('edge', {
            'id': f'e{eid}',
            'source': source,
            'target': target
        })
        self.tb.end('edge')


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


def graph(dbc, out, opts):
    cluster = opts['CLUSTER']
    _log.info('exporting graph for cluster %s', cluster)

    format = opts.get('--format', 'gml')
    if format == 'gml':
        gw = GMLWriter(out)
    elif format == 'graphml':
        gw = GraphMLWriter(out)
    else:
        raise ValueError('invalid format ' + format)
    gw.start()
    gw.node_attr('label')
    gw.node_attr('category')
    gw.node_attr('title')

    gl = GraphLoader()

    with dbc.cursor() as cur:
        gl.set_cluster(cluster, dbc)

        _log.info('fetching ISBNs')
        cur.execute(gl.q_isbns())
        for iid, isbn in cur:
            gw.node(id=f'i{iid}', label=isbn, category='ISBN')

        _log.info('fetching LOC records')
        cur.execute(gl.q_loc_nodes(True))
        for rid, title in cur:
            gw.node(id=f'l{rid}', label=rid, category='LOC', title=title)

        _log.info('fetching LOC ISBN links')
        cur.execute(gl.q_loc_edges())
        for iid, rid in cur:
            gw.edge(source=f'l{rid}', target=f'i{iid}')

        _log.info('fetching OL editions')
        cur.execute(gl.q_ol_edition_nodes(True))
        for eid, ek, e_title in cur:
            gw.node(id=f'ole{eid}', label=ek, category='OLE', title=e_title)

        _log.info('fetching OL works')
        cur.execute(gl.q_ol_work_nodes(True))
        for wid, wk, w_title in cur:
            gw.node(id=f'olw{wid}', label=wk, category='OLW', title=w_title)

        _log.info('fetching OL ISBN edges')
        cur.execute(gl.q_ol_edition_edges())
        for iid, eid in cur:
            gw.edge(source=f'ole{eid}', target=f'i{iid}')

        _log.info('fetching OL edition/work edges')
        cur.execute(gl.q_ol_work_edges())
        for eid, wid in cur:
            gw.edge(source=f'ole{eid}', target=f'olw{wid}')

        _log.info('fetching GR books')
        cur.execute(gl.q_gr_book_edges())
        bids = set()
        for iid, bid in cur:
            if bid not in bids:
                gw.node(id=f'grb{bid}', label=bid, category='GRB')
                bids.add(bid)
            gw.edge(source=f'grb{bid}', target=f'i{iid}')

        _log.info('fetching GR works')
        cur.execute(gl.q_gr_work_nodes(True))
        for wid, title in cur:
            gw.node(id=f'grw{wid}', label=wid, category='GRW', title=title)

        _log.info('fetching GR work/edition edges')
        cur.execute(gl.q_gr_work_edges())
        for bid, wid in cur:
            gw.edge(source=f'grw{wid}', target=f'grb{bid}')

    gw.finish()
    _log.info('exported graph')


def full_graph(opts):
    gl = GraphLoader()
    with db.engine().connect() as cxn:
        g = gl.load_minimal_graph(cxn)


    ofn = opts['-o']
    _log.info('saving graph to %s', ofn)
    g.save(ofn)


_log = script_log(__name__)
opts = docopt(__doc__)

if opts['-o']:
    out = open(opts['-o'], 'w', encoding='utf8')
else:
    out = sys.stdout

if opts['--full-graph']:
    full_graph(opts)
else:
    with db.connect() as dbc:
        if opts['--stats']:
            stats(dbc, out, opts)
        elif opts['--records']:
            records(dbc, out, opts)
        elif opts['--graph']:
            graph(dbc, out, opts)
