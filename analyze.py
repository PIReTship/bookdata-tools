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
    inos = isbns.loc[:, ['isbn_id', 'ino']].set_index('isbn_id')
    intbl = isbn_recs.join(inos, on='isbn_id')

    _log.info('making edge table from %d rows', len(intbl))
    intbl = intbl.loc[:, ['record', 'ino']]
    intbl = intbl.set_index('record')
    edges = intbl.join(intbl, lsuffix='_left', rsuffix='_right')

    _log.info('clustering')
    iters = _make_clusters(isbns.cluster.values, edges.ino_left.values, edges.ino_right.values)
    _log.info('produced %d clusters in %d iterations', isbns.cluster.nunique(), iters)
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


@task(s.init)
def cluster_loc(c, force=False):
    "Cluster ISBNs using only the LOC data"
    s.check_prereq('loc-index')
    s.start('loc-cluster', force=force)
    _log.info('reading LOC ISBN records')
    loc_isbn_recs = pd.read_sql('''
        SELECT isbn_id, rec_id AS record
        FROM loc_rec_isbn
    ''', s.db_url())
    _log.info('clustering %d ISBN records', len(loc_isbn_recs))
    loc_clusters = cluster_isbns(loc_isbn_recs)
    _log.info('writing ISBN records')
    loc_clusters[['isbn_id', 'cluster']].to_csv(s.data_dir / 'clusters-loc.csv', index=False, header=False)
    _log.info('importing ISBN records')
    _import_clusters('loc_isbn_cluster', s.data_dir / 'clusters-loc.csv')
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
    loc_clusters[['isbn_id', 'cluster']].to_csv(s.data_dir / 'clusters-ol.csv', index=False, header=False)
    _log.info('importing ISBN records')
    _import_clusters('ol_isbn_cluster', s.data_dir / 'clusters-ol.csv')
    s.finish('ol-cluster')


@task(s.init)
def cluster_gr(c, force=False):
    "Cluster ISBNs using only the GoodReads data"
    s.check_prereq('gr-index-books')
    s.start('gr-cluster')
    _log.info('reading GoodReads ISBN records')
    gr_isbn_recs = pd.read_sql('''
        SELECT isbn_id, COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id)) AS record
        FROM gr_book_isbn JOIN gr_book_ids USING (gr_book_id)
    ''', s.db_url())
    _log.info('clustering %d ISBN records', len(gr_isbn_recs))
    loc_clusters = cluster_isbns(gr_isbn_recs)
    _log.info('writing ISBN records')
    loc_clusters[['isbn_id', 'cluster']].to_csv(s.data_dir / 'clusters-gr.csv', index=False, header=False)
    _log.info('importing ISBN records')
    _import_clusters('gr_isbn_cluster', s.data_dir / 'clusters-gr.csv')
    s.finish('gr-cluster')


@task(s.init)
def cluster(c, force=False):
    "Cluster ISBNs"
    s.check_prereq('gr-index')
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
    _log.info('reading GoodReads ISBN records')
    gr_isbn_recs = pd.read_sql('''
        SELECT isbn_id, COALESCE(bc_of_gr_work(gr_work_id), bc_of_gr_book(gr_book_id)) AS record
        FROM gr_book_isbn JOIN gr_book_ids USING (gr_book_id)
    ''', s.db_url())
    all_isbn_recs = pd.concat([
        loc_isbn_recs.assign(record=lambda df: df.record + s.numspaces['rec']),
        ol_isbn_recs,
        gr_isbn_recs
    ])

    _log.info('clustering %d ISBN records', len(all_isbn_recs))
    loc_clusters = cluster_isbns(all_isbn_recs)
    _log.info('writing ISBN records')
    loc_clusters.to_csv(s.data_dir / 'clusters.csv', index=False, header=False)
    _log.info('importing ISBN records')
    _import_clusters('isbn_cluster', s.data_dir / 'clusters.csv')
    s.finish('cluster')
