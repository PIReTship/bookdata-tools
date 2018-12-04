import os
import logging
import subprocess as sp
import gzip
import threading
from textwrap import dedent
from functools import reduce
from humanize import naturalsize, intcomma
from psycopg2 import sql

import pandas as pd
import numpy as np
from numba import njit

import support as s
from invoke import task

_log = logging.getLogger(__name__)


class scope_locmds:
    name = 'LOC-MDS'
    prereq = 'loc-mds-book-index'

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
    prereq = 'ol-index'

    node_query = dedent('''
        SELECT DISTINCT isbn_id, MIN(book_code) AS record
        FROM ol.isbn_link GROUP BY isbn_id
    ''')

    edge_query = dedent('''
        SELECT DISTINCT l.isbn_id AS left_isbn, r.isbn_id AS right_isbn
        FROM ol.isbn_link l JOIN ol.isbn_link r ON (l.book_code = r.book_code)
    ''')

_all_scopes = ['locmds', 'ol']

def get_scope(name):
    return globals()[f'scope_{name}']


class _LoadThread(threading.Thread):
    """
    Thread worker for copying database results to a stream we can read.
    """
    def __init__(self, dbc, query, dir='out'):
        super().__init__()
        self.database = dbc
        self.query = query
        rfd, wfd = os.pipe()
        self.reader = os.fdopen(rfd)
        self.writer = os.fdopen(wfd, 'w')
        self.chan = self.writer if dir == 'out' else self.reader

    def run(self):
        with self.chan, self.database.cursor() as cur:
            cur.copy_expert(self.query, self.chan)


def load_table(dbc, query):
    cq = sql.SQL('COPY ({}) TO STDOUT WITH CSV HEADER')
    q = sql.SQL(query)
    thread = _LoadThread(dbc, cq.format(q))
    thread.start()
    data = pd.read_csv(thread.reader)
    thread.join()
    return data


def save_table(dbc, table, data: pd.DataFrame):
    cq = sql.SQL('COPY {} FROM STDIN WITH CSV')
    thread = _LoadThread(dbc, cq.format(table), 'in')
    thread.start()
    data.to_csv(thread.writer, header=False, index=False)
    thread.writer.close()
    thread.join()


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
              intcomma(isbns.cluster.nunique()), iters)
    return isbns.loc[:, ['isbn_id', 'cluster']]


@njit
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
        nchanged = 0
        iters = iters + 1
        for i in range(len(ls)):
            left = ls[i]
            right = rs[i]
            if clusters[left] < clusters[right]:
                clusters[right] = clusters[left]
                nchanged += 1

    return iters


def _import_clusters(dbc, schema, frame):
    schema_i = sql.Identifier(schema)
    with dbc, dbc.cursor() as cur:
        _log.info('creating cluster table')
        cur.execute(sql.SQL('DROP TABLE IF EXISTS {}.isbn_cluster CASCADE').format(schema_i))
        cur.execute(sql.SQL('''
            CREATE TABLE {}.isbn_cluster (
                isbn_id INTEGER NOT NULL,
                cluster INTEGER NOT NULL
            )
        ''').format(schema_i))
        _log.info('loading %d clusters into %s.isbn_cluster', len(frame), schema)
        save_table(dbc, sql.SQL('{}.isbn_cluster').format(schema_i), frame)
        cur.execute(sql.SQL('ALTER TABLE {}.isbn_cluster ADD PRIMARY KEY (isbn_id)').format(schema_i))
        cur.execute(sql.SQL('CREATE INDEX isbn_cluster_idx ON {}.isbn_cluster (cluster)').format(schema_i))
        cur.execute(sql.SQL('ANALYZE {}.isbn_cluster').format(schema_i))


@task(s.init)
def cluster(c, scope=None, force=False):
    "Cluster ISBNs"
    with s.database(autocommit=True) as db:
        if scope is None:
            step = 'cluster'
            schema = 'public'
            scopes = _all_scopes
        else:
            step = f'{scope}-cluster'
            schema = scope
            scopes = [scope]

        for scope in scopes:
            s.check_prereq(get_scope(scope).prereq)

        s.start(step, force=force)

        isbn_recs = []
        isbn_edges = []
        for scope in scopes:
            sco = get_scope(scope)
            _log.info('reading ISBNs for %s', scope)
            isbn_recs.append(load_table(db, sco.node_query))

            _log.info('reading edges for %s', scope)
            isbn_edges.append(load_table(db, sco.edge_query))

        isbn_recs = pd.concat(isbn_recs, ignore_index=True)
        isbn_edges = pd.concat(isbn_edges, ignore_index=True)

        _log.info('clustering %s ISBN records with %s edges',
                  intcomma(len(isbn_recs)), intcomma(len(isbn_edges)))
        loc_clusters = cluster_isbns(isbn_recs, isbn_edges)
        _log.info('saving cluster records to database')
        _import_clusters(db, schema, loc_clusters)
        s.finish(step)


@task(s.init)
def book_authors(c, force=False):
    "Analyze book authors"
    s.check_prereq('az-index')
    s.check_prereq('bx-index')
    s.check_prereq('viaf-index')
    s.check_prereq('loc-mds-book-index')
    s.start('book-authors', force=force)
    _log.info('Analzye book authors')
    s.psql(c, 'author-info.sql', True)
    s.finish('book-authors')
