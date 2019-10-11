"""
Usage:
    cluster.py [-T FILE] [SCOPE]

Options:
    -T FILE
        Write transcript to FILE.
    SCOPE
        Cluster SCOPE.
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

from bookdata import db, script_log

_log = script_log(__name__)


class scope_loc_mds:
    name = 'LOC-MDS'
    schema = 'locmds'

    node_query = dedent('''
        SELECT isbn_id, MIN(bc_of_loc_rec(rec_id)) AS record
        FROM locmds.book_rec_isbn GROUP BY isbn_id
    ''')

    edge_query = dedent('''
        SELECT DISTINCT l.isbn_id AS left_isbn, r.isbn_id AS right_isbn
        FROM locmds.book_rec_isbn l JOIN locmds.book_rec_isbn r ON (l.rec_id = r.rec_id)
    ''')


class scope_ol:
    name = 'OpenLibrary'
    schema = 'ol'

    node_query = dedent('''
        SELECT isbn_id, MIN(book_code) AS record
        FROM ol.isbn_link GROUP BY isbn_id
    ''')

    edge_query = dedent('''
        SELECT DISTINCT l.isbn_id AS left_isbn, r.isbn_id AS right_isbn
        FROM ol.isbn_link l JOIN ol.isbn_link r ON (l.book_code = r.book_code)
    ''')


class scope_gr:
    name = 'GoodReads'
    schema = 'gr'

    node_query = dedent('''
        SELECT DISTINCT isbn_id, MIN(book_code) AS record
        FROM gr.book_isbn GROUP BY isbn_id
    ''')

    edge_query = dedent('''
        SELECT DISTINCT l.isbn_id AS left_isbn, r.isbn_id AS right_isbn
        FROM gr.book_isbn l JOIN gr.book_isbn r ON (l.book_code = r.book_code)
    ''')


class scope_loc_id:
    name = 'LOC'
    schema = 'locid'

    node_query = dedent('''
        SELECT isbn_id, MIN(book_code) AS record
        FROM locid.isbn_link GROUP BY isbn_id
    ''')

    edge_query = dedent('''
        SELECT DISTINCT l.isbn_id AS left_isbn, r.isbn_id AS right_isbn
        FROM locid.isbn_link l JOIN locid.isbn_link r ON (l.book_code = r.book_code)
    ''')


_all_scopes = ['ol', 'gr', 'loc-mds']


def get_scope(name):
    n = name.replace('-', '_')
    return globals()[f'scope_{n}']


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


def _import_clusters(dbc, schema, frame):
    with dbc.cursor() as cur:
        schema_i = sql.Identifier(schema)
        _log.info('creating cluster table')
        cur.execute(sql.SQL('DROP TABLE IF EXISTS {}.isbn_cluster CASCADE').format(schema_i))
        cur.execute(sql.SQL('''
            CREATE TABLE {}.isbn_cluster (
                isbn_id INTEGER NOT NULL,
                cluster INTEGER NOT NULL
            )
        ''').format(schema_i))
        _log.info('loading %d clusters into %s.isbn_cluster', len(frame), schema)
        db.save_table(dbc, sql.SQL('{}.isbn_cluster').format(schema_i), frame)
        cur.execute(sql.SQL('ALTER TABLE {}.isbn_cluster ADD PRIMARY KEY (isbn_id)').format(schema_i))
        cur.execute(sql.SQL('CREATE INDEX isbn_cluster_idx ON {}.isbn_cluster (cluster)').format(schema_i))
        cur.execute(sql.SQL('ANALYZE {}.isbn_cluster').format(schema_i))


def _hash_frame(df):
    hash = hashlib.md5()
    for c in df.columns:
        hash.update(df[c].values.data)
    return hash.hexdigest()


def cluster(scope, txout):
    "Cluster ISBNs"
    with db.connect() as dbc:
        _log.info('preparing to cluster scope %s', scope)
        if scope:
            step = f'{scope}-cluster'
            schema = get_scope(scope).schema
            scopes = [scope]
        else:
            step = 'cluster'
            schema = 'public'
            scopes = _all_scopes

        with dbc:
            db.begin_stage(dbc, step)

            isbn_recs = []
            isbn_edges = []
            for scope in scopes:
                sco = get_scope(scope)
                _log.info('reading ISBNs for %s', scope)
                irs = db.load_table(dbc, sco.node_query)
                n_hash = _hash_frame(irs)
                isbn_recs.append(irs)
                print('READ NODES', scope, n_hash, file=txout)

                _log.info('reading edges for %s', scope)
                ies = db.load_table(dbc, sco.edge_query)
                e_hash = _hash_frame(ies)
                isbn_edges.append(ies)
                print('READ EDGES', scope, e_hash, file=txout)

            isbn_recs = pd.concat(isbn_recs, ignore_index=True)
            isbn_edges = pd.concat(isbn_edges, ignore_index=True)

            _log.info('clustering %s ISBN records with %s edges',
                      number(len(isbn_recs)), number(len(isbn_edges)))
            loc_clusters = cluster_isbns(isbn_recs, isbn_edges)
            _log.info('saving cluster records to database')
            _import_clusters(dbc, schema, loc_clusters)

            c_hash = _hash_frame(loc_clusters)
            print('WRITE CLUSTERS', c_hash, file=txout)

            db.end_stage(dbc, step, c_hash)


opts = docopt(__doc__)
tx_fn = opts.get('-T', None)
scope = opts.get('SCOPE', None)

if tx_fn == '-' or not tx_fn:
    tx_out = sys.stdout
else:
    _log.info('writing transcript to %s', tx_fn)
    tx_out = open(tx_fn, 'w')

cluster(scope, tx_out)
