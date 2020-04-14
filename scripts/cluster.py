"""
Usage:
    cluster.py [-T FILE]

Options:
    -T FILE
        Write transcript to FILE.
"""
import os
import sys
import gzip
import threading
from textwrap import dedent
from functools import reduce
from natural.number import number
from psycopg2 import sql
import hashlib
from docopt import docopt

import pandas as pd
import numpy as np

from graph_tool.all import label_components

from bookdata import db, tracking, script_log
from bookdata.graph import GraphLoader
from bookdata.schema import *

_log = script_log(__name__)


def cluster_isbns(isbn_recs, edges):
    """
    Compute ISBN clusters.
    """
    _log.info('initializing isbn vector')
    isbns = isbn_recs.groupby('isbn_id').record.min()
    index = isbns.index
    clusters = isbns.values

    _log.info('mapping edge IDs')
    edges = edges.assign(left_ino=index.get_indexer(edges.left_isbn).astype('i4'))
    assert np.all(edges.left_ino >= 0)
    edges = edges.assign(right_ino=index.get_indexer(edges.right_isbn).astype('i4'))
    assert np.all(edges.right_ino >= 0)

    _log.info('clustering')
    iters = _make_clusters(clusters, edges.left_ino.values, edges.right_ino.values)
    isbns = isbns.reset_index(name='cluster')
    _log.info('produced %s clusters in %d iterations',
              number(isbns.cluster.nunique()), iters)
    return isbns.loc[:, ['isbn_id', 'cluster']]


def _make_clusters(clusters, ls, rs):
    """
    Compute book clusters.  The input is initial cluster assignments and the left and right
    indexes for co-occuring ISBN edges; these are ISBNs that have connections to the same
    record in the bipartite ISBN-record graph.

    Args:
        clusters(ndarray): the initial cluster assignments
        ls(ndarray): the indexes of the left hand side of edges
        rs(ndarray): the indexes of the right hand side of edges
    """
    iters = 0
    nchanged = len(ls)

    while nchanged > 0:
        iters = iters + 1
        cdf = pd.DataFrame({
            'idx': rs,
            'cluster': np.minimum(clusters[ls], clusters[rs])
        })
        c = cdf.groupby('idx')['cluster'].min()
        nchanged = np.sum(c.values != clusters[c.index])
        _log.info('iteration %d changed %d clusters', iters, nchanged)
        clusters[c.index] = c.values

    return iters


def _import_clusters(dbc, frame):
    with dbc.cursor() as cur:
        _log.info('creating cluster table')
        cur.execute(sql.SQL('DROP TABLE IF EXISTS isbn_cluster CASCADE'))
        cur.execute(sql.SQL('''
            CREATE TABLE isbn_cluster (
                isbn_id INTEGER NOT NULL,
                cluster INTEGER NOT NULL
            )
        '''))
        _log.info('loading %d clusters into isbn_cluster', len(frame))

    db.save_table(dbc, sql.SQL('isbn_cluster'), frame)
    with dbc.cursor() as cur:
        cur.execute(sql.SQL('ALTER TABLE isbn_cluster ADD PRIMARY KEY (isbn_id)'))
        cur.execute(sql.SQL('CREATE INDEX isbn_cluster_idx ON isbn_cluster (cluster)'))
        cur.execute(sql.SQL('ANALYZE isbn_cluster'))


def _hash_frame(df):
    hash = hashlib.md5()
    for c in df.columns:
        hash.update(df[c].values.data)
    return hash.hexdigest()


def cluster(txout):
    "Cluster ISBNs"
    with db.connect() as dbc, dbc:
        tracking.begin_stage(dbc, 'cluster')

        with db.engine().connect() as cxn:
            _log.info('loading graph')
            gl = GraphLoader()
            g = gl.load_graph(cxn, False)

        print('NODES', g.num_vertices(), file=txout)
        print('EDGES', g.num_edges(), file=txout)

        _log.info('finding connected components')
        comps, hist = label_components(g)
        _log.info('found %d components, largest has %s items', len(hist), np.max(hist))
        print('COMPONENTS', len(hist), file=txout)

        _log.info('saving cluster records to database')
        is_isbn = g.vp.source.a == ns_isbn.code
        clusters = pd.DataFrame({
            'isbn_id': g.vp.label.a[is_isbn],
            'cluster': comps.a[is_isbn]
        })
        _import_clusters(dbc, clusters)

        _log.info('saving ID graph')
        g.vp['cluster'] = comps
        g.save('data/id-graph.gt')

        c_hash = _hash_frame(clusters)
        print('WRITE CLUSTERS', c_hash, file=txout)

        tracking.end_stage(dbc, 'cluster', c_hash)


opts = docopt(__doc__)
tx_fn = opts.get('-T', None)

if tx_fn == '-' or not tx_fn:
    tx_out = sys.stdout
else:
    _log.info('writing transcript to %s', tx_fn)
    tx_out = open(tx_fn, 'w')

cluster(tx_out)
