import logging
import subprocess as sp

import pandas as pd
import numpy as np
from numba import njit

import support as s
from invoke import task

_log = logging.getLogger(__name__)


def cluster_isbns(isbn_recs):
    """
    Compute ISBN clusters.
    """
    _log.info('initializing isbn vector')
    isbns = isbn_recs.groupby('isbn_id').record.min()
    isbns = isbns.reset_index(name='cluster')
    isbns['ino'] = np.arange(len(isbns), dtype=np.int32)
    intbl = pd.merge(isbn_recs, isbns.loc[:, ['isbn_id', 'ino']])
    left = intbl.loc[:, ['record', 'ino']].rename(columns={'ino': 'left'})
    right = intbl.loc[:, ['record', 'ino']].rename(columns={'ino': 'right'})
    _log.info('making edge table')
    edges = pd.merge(left, right)
    _log.info('clustering')
    iters = _make_clusters(isbns.cluster.values, edges.left.values, edges.right.values)
    _log.info('clustered in', iters, 'iterations')
    return isbns


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
    kid = sp.Popen(['psql', '-a'], stdin=sp.PIPE)
    kid.stdin.write(sql)
    kid.communicate()
    if kid.wait() != 0:
        _log.error('psql exited with code %d', rc)
        raise RuntimeError('psql error')


@task(s.init)
def cluster_loc(c, force=False):
    "Cluster ISBNs using only the LOC data"
    s.check_prereq('loc-index')
    s.start('loc-cluster')
    _log.info('reading LOC ISBN records')
    loc_isbn_recs = pd.read_sql('''
        SELECT isbn_id, rec_id AS record
        FROM loc_rec_isbn
    ''', s.db_url())
    _log.info('clustering %d ISBN records', len(loc_isbn_recs))
    loc_clusters = cluster_isbns(loc_isbn_recs)
    _log.info('writing ISBN records')
    loc_clusters.to_csv(s.data_dir / 'clusters-loc.csv', index=False, header=False)
    _log.info('importing ISBN records')
    _import_clusters('loc_sbn_cluster', s.data_dir / 'clusters-loc.csv')
    s.finish('loc-cluster')


@task(s.init)
def cluster_ol(c, force=False):
    "Cluster ISBNs using only the OpenLibrary data"
    s.check_prereq('ol-index')
    s.start('ol-cluster')
    _log.info('reading OpenLibrary ISBN records')
    ol_isbn_recs = pd.read_sql('''
        SELECT isbn_id, book_code AS record
        FROM ol_isbn_link
    ''', s.db_url())
    _log.info('clustering %d ISBN records', len(ol_isbn_recs))
    loc_clusters = cluster_isbns(ol_isbn_recs)
    _log.info('writing ISBN records')
    loc_clusters.to_csv(s.data_dir / 'clusters-ol.csv', index=False, header=False)
    _log.info('importing ISBN records')
    _import_clusters('ol_isbn_cluster', s.data_dir / 'clusters-ol.csv')
    s.finish('loc-cluster')

@task(s.init)
def cluster(c, force=False):
    "Cluster ISBNs"
    s.check_prereq('ol-index')
    s.check_prereq('loc-index')
    s.start('cluster')
    
    _log.info('reading LOC ISBN records')
    loc_isbn_recs = pd.read_sql('''
        SELECT isbn_id, rec_id AS record
        FROM loc_rec_isbn
    ''', s.db_url())
    _log.info('reading OpenLibrary ISBN records')
    ol_isbn_recs = pd.read_sql('''
        SELECT isbn_id, book_code AS record
        FROM ol_isbn_link
    ''', s.db_url())
    all_isbn_recs = pd.concat([
        loc_rec_isbns.assign(record=lambda df: df.record + numspaces['rec']),
        ol_rec_edges
    ])

    _log.info('clustering %d ISBN records', len(all_isbn_recs))
    loc_clusters = cluster_isbns(all_isbn_recs)
    _log.info('writing ISBN records')
    loc_clusters.to_csv(s.data_dir / 'clusters.csv', index=False, header=False)
    _log.info('importing ISBN records')
    _import_clusters('isbn_cluster', s.data_dir / 'clusters.csv')
    s.finish('cluster')
