import logging
import subprocess as sp
import gzip
from functools import reduce
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
        SELECT l.isbn_id AS left_isbn, r.isbn_id AS right_isbn
        FROM ol_isbn_link l JOIN ol_isbn_link r ON (l.book_code = r.book_code)
    ''',
    'gr': '''
        SELECT l.isbn_id AS left_isbn, r.isbn_id AS right_isbn
        FROM gr_book_isbn l JOIN gr_book_isbn r ON (l.book_code = r.book_code)
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


def _export_isbns(scope, file):
    query = rec_queries[scope]
    if file.exists():
        _log.info('%s already exists, not re-exporting', file)
        return
    _log.info('exporting ISBNs from %s to %s', rec_names[scope], file)
    tmp = file.with_name('.tmp.' + file.name)
    with s.database(autocommit=True) as db, db.cursor() as cur, gzip.open(tmp, 'w', 4) as out:
        cur.copy_expert(f'COPY ({query}) TO STDOUT WITH CSV HEADER', out)
    tmp.replace(file)


def _export_edges(scope, file):
    query = rec_edge_queries[scope]
    if file.exists():
        _log.info('%s already exists, not re-exporting', file)
        return
    _log.info('exporting ISBN-ISBN edges from %s to %s', rec_names[scope], file)
    tmp = file.with_name('.tmp.' + file.name)
    with s.database(autocommit=True) as db, db.cursor() as cur, gzip.open(tmp, 'w', 4) as out:
        cur.copy_expert(f'COPY ({query}) TO STDOUT WITH CSV HEADER', out)
    tmp.replace(file)


def _import_clusters(tbl, file):
    sql = f'''
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
        fn = f'{scope}-clusters.csv'
        table = f'{scope}_isbn_cluster'
        scopes = [scope]

    for scope in scopes:
        s.check_prereq(prereqs[scope])

    s.start(step, force=force)

    isbn_recs = []
    isbn_edges = []
    for scope in scopes:
        i_fn = s.data_dir / f'{scope}-isbns.csv.gz'
        _export_isbns(scope, i_fn)
        _log.info('reading ISBNs from %s', i_fn)
        isbn_recs.append(pd.read_csv(i_fn))

        e_fn = s.data_dir / f'{scope}-edges.csv.gz'
        _export_edges(scope, e_fn)
        _log.info('reading edges from %s', e_fn)
        isbn_edges.append(pd.read_csv(e_fn))

    isbn_recs = pd.concat(isbn_recs, ignore_index=True)
    isbn_edges = pd.concat(isbn_edges, ignore_index=True)

    _log.info('clustering %s ISBN records with %s edges',
              intcomma(len(isbn_recs)), intcomma(len(isbn_edges)))
    loc_clusters = cluster_isbns(isbn_recs, isbn_edges)
    _log.info('writing ISBN records to %s', fn)
    loc_clusters.to_csv(s.data_dir / fn, index=False, header=False)
    _log.info('importing ISBN records')
    _import_clusters(table, s.data_dir / fn)
    s.finish(step)
