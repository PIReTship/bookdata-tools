import logging
import subprocess as sp
from humanize import naturalsize, intcomma

import pandas as pd
import numpy as np
from numba import njit

import support as s
from invoke import task

_log = logging.getLogger(__name__)

rec_names = {'loc': 'LOC', 'ol': 'OpenLibrary', 'gr': 'GoodReads'}
rec_queries = {
    'loc': '''
        SELECT isbn_id, bc_of_loc_rec(rec_id) AS record
        FROM loc_rec_isbn
    ''',
    'ol': '''
        SELECT isbn_id, book_code AS record
        FROM ol_isbn_link
    ''',
    'gr': '''
        SELECT isbn_id, book_code AS record
        FROM gr_book_isbn
    '''
}
rec_edge_queries = {
    'loc': '''
        SELECT l.isbn_id AS left_isbn, r.isbn_id AS right_isbn
        FROM loc_rec_isbn l JOIN loc_rec_isbn r ON (l.rec_id = r.rec_id)
    ''',
    'ol': '''
        SELECT isbn_id, book_code AS record
        FROM ol_isbn_link
    ''',
    'gr': '''
        SELECT isbn_id, book_code AS record
        FROM gr_book_isbn
    '''
}
prereqs = {'loc': 'loc-index', 'ol': 'ol-index', 'gr': 'gr-index-books'}


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
              intcommas(isbns.cluster.nunique()), iters)
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


def _import_clusters(tbl, file):
    sql = f'''
        \\set on_error_stop true
        DROP TABLE IF EXISTS {tbl} CASCADE;
        CREATE TABLE {tbl} (
            isbn_id INTEGER NOT NULL,
            cluster INTEGER NOT NULL
        );
        \copy {tbl} FROM '{file}' WITH (FORMAT CSV);
        ALTER TABLE {tbl} ADD PRIMARY KEY (isbn_id);
        CREATE INDEX {tbl}_idx ON {tbl} (cluster);
        ANALYZE {tbl};
    '''
    _log.info('running psql for %s', tbl)
    kid = sp.Popen(['psql', '-v', 'ON_ERROR_STOP=on', '-a'], stdin=sp.PIPE)
    kid.stdin.write(sql.encode('ascii'))
    kid.communicate()
    rc = kid.wait()
    if rc:
        _log.error('psql exited with code %d', rc)
        raise RuntimeError('psql error')


def _read_recs(scope):
    _log.info('reading ISBN records from %s', rec_names[scope])
    recs = pd.read_sql(rec_queries[scope], s.db_url()).apply(lambda c: c.astype('i4'))
    _log.info('read %s ISBN records from %s (%s)', intcomma(len(recs)), rec_names[scope],
              naturalsize(recs.memory_usage(index=True, deep=True).sum()))
    
    return recs

def _read_edges(scope):
    _log.info('reading ISBN-ISBN edges from %s', rec_names[scope])
    edges = pd.read_sql(rec_edge_queries[scope], s.db_url()).apply(lambda c: c.astype('i4'))
    _log.info('read %s edges from %s (%s)', intcomma(len(edges)), rec_names[scope],
              naturalsize(edges.memory_usage(index=True, deep=True).sum()))

    return edges


@task(s.init)
def cluster(c, scope=None, force=False):
    "Cluster ISBNs"
    s.check_prereq('loc-index')
    s.check_prereq('ol-index')
    s.check_prereq('gr-index-books')
    if scope is None:
        step = 'cluster'
        fn = 'clusters.csv'
        table = 'isbn_cluster'
        scopes = list(rec_names.keys())
    else:
        step = f'{scope}-cluster'
        fn = f'clusters-{scope}.csv'
        table = f'{scope}_isbn_cluster'
        scopes = [scope]
    
    for scope in scopes:
        s.check_prereq(prereqs[scope])

    s.start(step, force=force)

    isbn_recs = pd.concat(_read_recs(scope) for scope in scopes)
    isbn_edges = pd.concat(_read_edges(scope) for scope in scopes)

    _log.info('clustering %s ISBN records with %s edges',
              intcomma(len(isbn_recs)), intcomma(len(isbn_edges)))
    loc_clusters = cluster_isbns(isbn_recs, isbn_edges)
    _log.info('writing ISBN records to %s', fn)
    loc_clusters.to_csv(s.data_dir / fn, index=False, header=False)
    _log.info('importing ISBN records')
    _import_clusters(table, s.data_dir / fn)
    s.finish(step)
