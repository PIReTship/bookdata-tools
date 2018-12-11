import logging
import subprocess as sp
from humanize import naturalsize

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
prereqs = {'loc': 'loc-index', 'ol': 'ol-index', 'gr': 'gr-index-books'}


def cluster_isbns(isbn_recs):
    """
    Compute ISBN clusters.
    """
    _log.info('initializing isbn vector')
    isbns = isbn_recs.groupby('isbn_id').record.min()
    isbns = isbns.reset_index(name='cluster')
    isbns['ino'] = np.arange(len(isbns), dtype=np.int32)
    inos = isbns.loc[:, ['isbn_id', 'ino']].set_index('isbn_id')
    intbl = isbn_recs.join(inos, on='isbn_id')
    _log.info('ISBN table takes %s', naturalsize(intbl.memory_usage(index=True, deep=True).sum()))

    _log.info('making edge table from %d rows', len(intbl))
    intbl = intbl.loc[:, ['record', 'ino']]
    intbl = intbl.set_index('record')
    edges = intbl.join(intbl, lsuffix='_left', rsuffix='_right')
    _log.info('edge table has %d rows in %s', len(edges),
              naturalsize(edges.memory_usage(index=True, deep=True).sum()))

    _log.info('clustering')
    iters = _make_clusters(isbns.cluster.values, edges.ino_left.values, edges.ino_right.values)
    _log.info('produced %d clusters in %d iterations', isbns.cluster.nunique(), iters)
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


def _read_edges(scope):
    _log.info('reading %s ISBN records', rec_names[scope])
    recs = pd.read_sql(rec_queries[scope], s.db_url()).apply(lambda c: c.astype('i4'))
    _log.info('read %d ISBN records from %s', len(recs), rec_names[scope])
    return recs


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
        s.check_prereq(f'{scope}-index')

    s.start(step, force=force)

    isbn_recs = pd.concat(_read_edges(scope) for scope in scopes)

    _log.info('clustering %d ISBN records', len(isbn_recs))
    loc_clusters = cluster_isbns(isbn_recs)
    _log.info('writing ISBN records to %s', fn)
    loc_clusters.to_csv(s.data_dir / fn, index=False, header=False)
    _log.info('importing ISBN records')
    _import_clusters(table, s.data_dir / fn)
    s.finish(step)
