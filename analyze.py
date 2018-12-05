import logging

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


@task(s.init)
def cluster_loc(c, force=False):
    "Cluster ISBNs using only the LOC data"
    _log.info('reading LOC ISBN records')
    loc_isbn_recs = pd.read_sql('''
        SELECT isbn_id, rec_id AS record
        FROM loc_rec_isbn
    ''', s.db_url())
    _log.info('clustering %d ISBN records', len(loc_isbn_recs))
    loc_clusters = cluster_isbns(loc_isbn_recs)
    _log.info('writing ISBN records')

@task(s.init)
def cluster(c, force=False):
    "Cluster ISBNs"
